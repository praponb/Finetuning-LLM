from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

line_level = (
    base.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("kwh").alias("min_reading_kwh"),
        F.count("*").alias("reading_count"),
    )
)

with_stats = (
    line_level.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.min_reading_kwh",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

with_details = (
    with_stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.reading_id",
        "s.reading_quality",
        "s.reading_read_at",
        "s.reading_kwh",
        "s.reading_count",
        "si.site_name",
    )
)

result = (
    with_details
    .filter(F.col("voltage") >= 216.8)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "reading_id",
        "reading_quality",
        "reading_read_at",
        "reading_kwh",
        "reading_count",
        "site_name",
        F.when(F.col("reading_count") >= 5, "large").when(F.col("reading_count") >= 2, "medium").otherwise("small").alias("reading_count_tier"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reading_read_at")).alias("days_since_reading_read_at"),
    )
    .orderBy("id")
)
