from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("temp_c", "id")
w_hist = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy("status_bits", "id")

enriched = (
    df_device_readings
    .filter(F.col("humidity") < 36)
    .withColumn("prev_temp_c", F.lag("temp_c", 1).over(w))
    .withColumn("status_bits_bucket", F.ntile(3).over(w_hist))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("temp_c") >= 41.8, "HIGH").otherwise("LOW").alias("temp_c_tier"),
    )
    .agg(
        F.min("status_bits_bucket").alias("min_status_bits_bucket"),
        F.max("pressure").alias("max_pressure"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .orderBy(F.col("max_pressure").desc())
)
