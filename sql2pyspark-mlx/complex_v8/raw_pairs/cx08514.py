from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")) & (F.col("quality") == "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

joined = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at"),
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
    )
)

with_counts = (
    joined.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at",
        "m.avg_reading_voltage",
    )
)

derived = (
    with_counts
    .filter(F.col("site_id").isNotNull())
    .select(
        "*",
        F.when(F.col("reading_count") >= 6, "large").when(F.col("reading_count") >= 3, "medium").otherwise("small").alias("reading_count_tier"),
    )
)

line_level_readings = (
    derived.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isin("OK", "BAD")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.reading_count",
        "s.avg_reading_voltage",
        "s.reading_count_tier",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    line_level_readings
    .select(
        "id",
        "site_id",
        "voltage",
        "reading_count",
        "avg_reading_voltage",
        "reading_count_tier",
        "reading_id",
        "reading_quality",
        "reading_read_at",
        (F.col("reading_read_at") + F.expr("INTERVAL 7 DAYS")).alias("reading_read_at_plus_7d"),
        F.least(F.col("avg_reading_voltage"), F.col("voltage")).alias("least_avg_reading_voltage_voltage"),
    )
    .orderBy(F.col("site_id").desc(), F.col("reading_count_tier").desc_nulls_first())
)
