from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("device_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("device_length").desc())
w_running = Window.partitionBy("customer_id").orderBy(F.col("device_length").asc_nulls_last())

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-08-25").cast("date")) & (F.col("country") == "TH"))
    .select("id", "customer_id", F.length("device").alias("device_length"))
)

result = (
    filtered
    .withColumn("cumulative_device_length", F.sum("device_length").over(w))
    .withColumn("customer_id_stddev_samp_device_length", F.stddev_samp("device_length").over(w_part))
    .withColumn("device_length_rank", F.rank().over(w3))
    .withColumn("customer_id_avg_device_length", F.avg("device_length").over(w_part))
    .withColumn("device_length_cume_dist", F.cume_dist().over(w_running))
)
