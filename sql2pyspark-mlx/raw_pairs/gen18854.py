from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("island_id_tier").orderBy(F.col("num_device_codes").desc())

with_gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "firmware",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("island_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
        F.when(F.col("humidity") >= 75, "HIGH").otherwise("LOW").alias("humidity_tier"),
    )
    .agg(
        F.max("reading_ts").alias("last_reading_ts"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
    )
    .withColumn("num_device_codes_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("dv_04_rows").desc())
)
