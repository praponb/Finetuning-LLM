from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())

result = (
    df_device_readings
    .withColumn("humidity_bucket", F.ntile(4).over(w))
    .withColumn("humidity_avg_2", F.avg("humidity").over(w2))
    .withColumn("status_bits_dense_rank", F.dense_rank().over(w3))
)
