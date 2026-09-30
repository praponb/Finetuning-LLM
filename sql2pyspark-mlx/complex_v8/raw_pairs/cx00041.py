from pyspark.sql import functions as F

cleaned = (
    df_sites
    .filter(F.upper(F.trim(F.col("site_name"))).like("M%"))
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
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("reading_quality_count"),
        F.max("read_at").alias("last_reading_read_at"),
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
    )
)

profiled = (
    cleaned.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.last_reading_read_at",
        "m.avg_reading_kwh",
    )
)

line_level_readings = (
    profiled.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.zone",
        "s.tariff",
        "s.installed_kw",
        "s.commissioned_on",
        "s.reading_quality_count",
        "s.last_reading_read_at",
        "s.avg_reading_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count_2"),
    )
)

summarized_rows = (
    line_level_readings.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count_2"), F.lit(0)).alias("reading_quality_count_2"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.id") == F.col("s.reading_id")) & (F.col("m.read_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.zone",
        "s.tariff",
        "s.installed_kw",
        "s.commissioned_on",
        "s.avg_reading_kwh",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_kwh",
        "s.reading_quality_count_2",
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage_2"),
        F.col("m.kwh").alias("reading_kwh_2"),
    )
)

result = (
    combined_details
    .rollup("tariff", "reading_quality_count_2")
    .agg(
        F.grouping("tariff").alias("is_total_tariff"),
        F.count(F.when(F.col("zone") == "Z3", 1)).alias("n_zone_z3"),
    )
    .select(
        F.coalesce(F.col("tariff"), F.lit("ALL")).alias("tariff"),
        "reading_quality_count_2",
        "is_total_tariff",
        "n_zone_z3",
    )
    .orderBy(
        F.col("tariff").asc_nulls_last(),
        F.col("reading_quality_count_2").asc_nulls_last(),
    )
)
