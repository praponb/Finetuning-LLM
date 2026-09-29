from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("status_bits", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("running_count_status_bits").desc_nulls_first())
w6 = Window.orderBy("reading_ts", "id")

valid = (
    df_device_readings
    .filter(F.col("humidity").isNotNull())
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.date_format("reading_ts", "MMM yyyy").alias("reading_ts_label"),
    )
)

gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

metrics = (
    islands
    .select(
        "*",
        F.avg("status_bits").over(w3).alias("running_avg_status_bits"),
        F.min("status_bits").over(w3).alias("running_min_status_bits"),
        F.count("status_bits").over(w3).alias("running_count_status_bits"),
        F.max("status_bits").over(w3).alias("running_max_status_bits"),
        F.ntile(5).over(w4).alias("status_bits_bucket"),
    )
)

metrics_device_readings = (
    metrics
    .filter(F.col("device_code").isin(["DV-02", "DV-04"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "reading_ts_label",
        "is_new_burst",
        "session_id",
        "running_avg_status_bits",
        "running_max_status_bits",
        F.dense_rank().over(w5).alias("running_count_status_bits_dense_rank"),
        F.lag("running_min_status_bits", 1).over(w6).alias("prev_running_min_status_bits"),
    )
)

result = (
    metrics_device_readings
    .withColumn("pressure_tier", F.when(F.col("pressure") >= 1030, "PREMIUM").otherwise("STANDARD"))
    .rollup("pressure_tier", "session_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.count("*").alias("cnt"),
        F.grouping("pressure_tier").alias("grouping_level"),
    )
    .orderBy(F.col("dv_04_count").desc())
)
