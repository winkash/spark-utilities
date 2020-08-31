import java.io.{File, IOException}
import java.net.InetAddress

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Try}

object LocalDiskHealthCheck {

  private val logger : Logger = LoggerFactory.getLogger(this.getClass)

  private type DiskResult = (String, Option[Exception])

  def diskCheck(sparkContext: SparkContext): Unit = {

    val partitions: Int = sparkContext.defaultParallelism // should be >= number of executors unless overridden explicitly

    // create an RDD with one partition on each executor
    val testRDD: RDD[Int] = sparkContext.parallelize(1 to partitions, partitions)

    // map RDD to failed/successful checks
    val results: RDD[DiskResult] = testRDD.mapPartitions(partition => checkDisks(partition.hashCode()).iterator).cache()

    // collect results:
    val success = results.collect { case (disk, None) => disk }.collect()
    val failures = results.collect { case (disk, Some(e)) => (disk, e) }.collect()

    // log results:
    success.foreach(disk => logger.info(s"Validated disk $disk"))
    failures.foreach { case (disk, e) => logger.error(s"Failed validation for disk $disk: ${e.getMessage}") }

    // throwing the first exception (if found) to get all info in HC page
    failures.headOption.foreach {
      case (disk, e) => throw new IOException(s"Failed validating disks, first failure on $disk, exception: ${e.getMessage}", e)
    }

  }

  // check all local disks on current node
  private def checkDisks(partitionId: Int): Seq[DiskResult] = {
    // loading executor's local configuration in search of that node's local-dirs
    // borrowed from org.apache.spark.util.Utils.getYarnLocalDirs
    val foldersVar: String = Option(System.getenv("YARN_LOCAL_DIRS"))
      .orElse(Option(System.getenv("LOCAL_DIRS")))
      .getOrElse(".")

    val folders: Seq[String] = foldersVar.split(",")

    folders.map(checkDisk(_, partitionId))
  }

  // check a specific disk, return disk description and the exception, if caught one
  private def checkDisk(folder: String, partitionId: Int): DiskResult = {
    val diskName = s"${InetAddress.getLocalHost}:$folder"
    val file = new File(folder, s"disk-HC-partition-$partitionId-time-${System.currentTimeMillis()}")

    val maybeException = Try { file.createNewFile(); file.delete() } match {
      case Failure(e: Exception) => Some(e)
      case _ => None
    }
    (diskName, maybeException)
  }
}
