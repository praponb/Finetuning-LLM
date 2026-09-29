from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

summarized_rows = (
    base_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at", "m.last_reading_read_at")
)

line_level = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.kwh_bucket",
        "s.first_reading_read_at",
        "s.last_reading_read_at",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
    )
)

with_counts = (
    line_level.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.avg_reading_kwh",
    )
)

result = (
    with_counts
    .filter(F.col("reading_read_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .select(
        "id",
        "read_at",
        "kwh",
        "kwh_bucket",
        "last_reading_read_at",
        "reading_voltage",
        "reading_read_at",
        "reading_count",
        "avg_reading_kwh",
        (F.col("reading_count") + F.col("reading_voltage")).alias("reading_count_plus_reading_voltage"),
        F.coalesce(F.col("kwh_bucket"), F.lit(0)).alias("kwh_bucket_filled"),
    )
    .orderBy(F.col("read_at").desc(), "id")
)
