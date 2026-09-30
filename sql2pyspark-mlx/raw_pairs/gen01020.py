from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("session_id").desc(), "id")
w6 = Window.orderBy("cumulative_session_id", "id").rowsBetween(-5, -1)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select("*", F.year("reading_ts").alias("reading_ts_year"))
)

gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

metrics = (
    islands
    .select(
        "id",
        "humidity",
        "session_id",
        F.sum("humidity").over(w3).alias("humidity_sum_2h"),
        F.sum("session_id").over(w4).alias("cumulative_session_id"),
    )
)

metrics_device_readings = (
    metrics
    .filter(F.col("cumulative_session_id") == 12)
    .select(
        "*",
        F.ntile(3).over(w5).alias("session_id_bucket"),
        F.avg("humidity").over(w6).alias("humidity_avg_5_prior"),
        F.ntile(10).over(w5).alias("session_id_bucket_2"),
    )
)

filtered = (
    metrics_device_readings
    .filter(F.col("session_id_bucket_2") <= 5)
    .withColumn("humidity_per_session_id", (F.col("humidity") / F.nullif("session_id", F.lit(0))))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("session_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("session_id_tier"),
    )
    .agg(
        F.percentile_approx("session_id_bucket", 0.5).alias("session_id_bucket_p50"),
        F.round(F.avg("humidity_sum_2h"), 2).alias("avg_humidity_sum_2h"),
        F.avg("humidity_per_session_id").alias("avg_humidity_per_session_id"),
        F.round(F.avg("cumulative_session_id"), 2).alias("avg_cumulative_session_id"),
        F.percentile_approx("session_id_bucket", [0.25, 0.5, 0.75]).alias("session_id_bucket_quartiles"),
    )
    .filter(F.col("session_id_bucket_p50") > 0)
    .orderBy("avg_humidity_sum_2h")
)
