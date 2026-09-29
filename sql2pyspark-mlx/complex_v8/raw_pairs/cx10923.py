from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.col("quality") == "OK") & (F.col("voltage") <= 235.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.abs(F.col("voltage") - 232.7).alias("voltage_distance"),
    )
)

without_matches = (
    recent
    .crossJoin(recent.agg(F.avg("voltage_distance").alias("overall_avg_voltage_distance")))
    .filter(F.col("voltage_distance") > F.col("overall_avg_voltage_distance"))
    .drop("overall_avg_voltage_distance")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

profiled = (
    without_matches.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

with_parent_site = (
    profiled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") < 386.6)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.voltage_distance",
        "s.reading_count",
        "s.reading_count_2",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_parent_site
    .groupingSets(
        [["site_id", "reading_count"], ["site_id"], []],
        "site_id", "reading_count",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.max("read_at").alias("last_read_at"),
        F.round(F.avg("reading_count_2"), 2).alias("avg_reading_count_2"),
    )
)
