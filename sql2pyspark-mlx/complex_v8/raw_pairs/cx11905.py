from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter((F.col("kwh") > 48.2) & (F.col("site_id") <= 6))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

summarized_rows = (
    base_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

enriched = (
    summarized_rows
    .filter(F.col("last_reading_read_at") >= F.lit("2025-01-17 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "last_reading_read_at",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        (F.col("read_at") + F.expr("INTERVAL 2 HOURS")).alias("read_at_plus_2h"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

profiled = (
    enriched.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

result = (
    profiled
    .select(
        "id",
        "read_at",
        "quality",
        "read_at_label",
        "first_reading_read_at",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy(F.col("quality_prefix").desc())
)
