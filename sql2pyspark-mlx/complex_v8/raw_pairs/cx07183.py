from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")) | (F.col("voltage") >= 223.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
    )
)

with_counts = (
    cleaned_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

detail_rows = (
    with_counts.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.reading_count",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

without_matches = (
    detail_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") <= 111.9) & (F.col("m.quality") != "EST"), "left_anti")
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "EST", 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count_2"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_stats = (
    without_matches.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
        "m.last_reading_read_at",
    )
)

scored = (
    with_stats
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reading_read_at")).alias("days_since_reading_read_at"),
        F.when(F.col("reading_quality_count") >= 4, "A").when(F.col("reading_quality_count") >= 2, "B").otherwise("C").alias("reading_quality_count_tier"),
    )
)

with_children = (
    scored.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.reading_count",
        "s.reading_id",
        "s.reading_read_at",
        "s.reading_quality_count",
        "s.last_reading_read_at",
        "s.reading_quality_count_tier",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.kwh").alias("reading_kwh_2"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality_2"),
    )
)

result = (
    with_children
    .select(
        "id",
        "voltage",
        "reading_count",
        "reading_id",
        "reading_read_at",
        "reading_quality_count",
        "reading_quality_count_tier",
        "reading_read_at_2",
        "reading_kwh_2",
        "reading_voltage",
        "reading_quality_2",
        F.round(F.col("reading_voltage") * 1.5, 2).alias("reading_voltage_adj"),
        F.when(F.col("reading_quality_2").isin("EST", "OK"), "group_a").when(F.col("reading_quality_2") == "BAD", "group_b").otherwise("other").alias("reading_quality_2_group"),
    )
    .orderBy(F.col("id").desc(), "reading_read_at_2")
)
