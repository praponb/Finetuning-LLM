from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter((F.col("voltage") < 232.0) & (F.col("kwh") < 101.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 223.3).alias("voltage_distance"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count(F.when(F.col("quality") != "EST", 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count_2"),
    )
)

with_stats = (
    base.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

with_activity = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (~F.col("m.quality").isin("EST", "OK")), "left_anti")
)

joined_site = (
    with_activity.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") > 96.9)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.reading_count",
        "s.reading_count_2",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

without_matches = (
    joined_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") > 43.1) & (F.col("m.quality").isNotNull()), "left_anti")
)

detail_rows = (
    without_matches.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.reading_count",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "reading_count",
        "reading_read_at",
        "reading_kwh",
        "reading_quality",
        "reading_voltage",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        F.coalesce(F.col("reading_voltage"), F.lit(0.0)).alias("reading_voltage_filled"),
    )
    .orderBy(F.col("reading_read_at").desc_nulls_first())
)
