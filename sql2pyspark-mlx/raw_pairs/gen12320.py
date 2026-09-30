from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .withColumn("firmware_prefix", F.substring("firmware", 1, 4))
    .withColumn("temp_c_tier", F.when(F.col("temp_c") >= 34.6, "High").when(F.col("temp_c") >= 21.2, "Medium").otherwise("Low"))
)

gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("hour_window").asc_nulls_first())
)
