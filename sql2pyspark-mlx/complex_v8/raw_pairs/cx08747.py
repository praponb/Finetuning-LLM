from pyspark.sql import functions as F

cleaned_sites = (
    df_sites
    .filter(F.col("commissioned_on").between(F.lit("2024-09-15").cast("date"), F.lit("2025-05-10").cast("date")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "EST", 1)).alias("reading_quality_count"),
        F.count(F.when(F.col("quality").isin("OK", "BAD"), 1)).alias("reading_quality_count_2"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

profiled = (
    cleaned_sites.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_quality_count_2"), F.lit(0)).alias("reading_quality_count_2"),
        "m.last_reading_read_at",
    )
)

with_activity = (
    profiled.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.to_date(F.col("m.read_at")) < F.lit("2025-02-12").cast("date")), "left_semi")
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at_2"),
        F.max("read_at").alias("last_reading_read_at_3"),
        F.count("*").alias("reading_count"),
    )
)

profiled_2 = (
    with_activity.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at_2",
        "m.last_reading_read_at_3",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

with_children_readings = (
    profiled_2.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.zone",
        "s.installed_kw",
        "s.reading_quality_count",
        "s.reading_quality_count_2",
        "s.last_reading_read_at",
        "s.last_reading_read_at_3",
        "s.reading_count",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

qualified = (
    with_children_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh") >= 45.3), "left_semi")
)

line_level = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.zone",
        "s.installed_kw",
        "s.reading_quality_count",
        "s.reading_quality_count_2",
        "s.last_reading_read_at",
        "s.last_reading_read_at_3",
        "s.reading_count",
        "s.reading_id",
        "s.reading_voltage",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.quality").alias("reading_quality_2"),
        F.col("m.voltage").alias("reading_voltage_2"),
    )
)

result = (
    line_level
    .select(
        "id",
        "installed_kw",
        "reading_quality_count",
        "reading_quality_count_2",
        "last_reading_read_at",
        "last_reading_read_at_3",
        "reading_count",
        "reading_id",
        "reading_voltage",
        "reading_id_2",
        "reading_read_at_2",
        "reading_quality_2",
        "reading_voltage_2",
    )
    .orderBy("reading_voltage_2")
)
