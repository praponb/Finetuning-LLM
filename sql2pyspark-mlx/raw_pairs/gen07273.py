from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("humidity", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.lit("2025-09-01").cast("date")) & (F.col("pressure") <= 1000))
    .select(
        "id",
        "humidity",
        "status_bits",
        "firmware",
        F.lower("firmware").alias("firmware_lower"),
        F.initcap(F.trim("device_code")).alias("device_code_title"),
    )
)

valid = (
    cleaned
    .filter(F.col("firmware").isin(["1.0.3", "1.1.0", "2.0.1"]))
    .withColumn("firmware_length", F.length("firmware"))
)

enriched = (
    valid
    .filter(F.col("firmware") == "1.0.3")
    .select(
        "id",
        "humidity",
        "status_bits",
        "device_code_title",
        F.max("humidity").over(w_running).alias("last7_max_humidity"),
        F.avg("humidity").over(w_running).alias("last7_avg_humidity"),
        F.sum("humidity").over(w_running).alias("last7_sum_humidity"),
        F.min("humidity").over(w_running).alias("last7_min_humidity"),
    )
)

base = (
    enriched
    .filter(F.col("last7_max_humidity") >= 59)
    .select("*", F.round("last7_max_humidity", 2).alias("last7_max_humidity_rounded"))
)

daily = (
    base
    .cube(
        F.when(F.col("last7_min_humidity") >= 66, "HIGH").otherwise("LOW").alias("last7_min_humidity_tier"),
        F.when(F.col("last7_max_humidity") >= 82, "LARGE").otherwise("SMALL").alias("last7_max_humidity_tier"),
    )
    .agg(
        F.percentile_approx("last7_min_humidity", [0.25, 0.5, 0.75]).alias("last7_min_humidity_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.sum("status_bits").alias("total_status_bits"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
    )
)

result = (
    daily
    .select(
        "last7_min_humidity_tier",
        "last7_max_humidity_tier",
        "last7_min_humidity_quartiles",
        "event_count",
        "total_status_bits",
        "status_bits_bit_and",
    )
    .orderBy(F.col("total_status_bits").desc())
)
