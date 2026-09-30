from pyspark.sql import functions as F

prepared = (
    df_sites
    .filter((F.col("tariff") != "EV") & (F.upper(F.trim(F.col("site_name"))).like("S%")))
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        F.length(F.trim(F.col("site_name"))).alias("site_name_len"),
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("voltage").alias("max_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

profiled = (
    prepared.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

enriched = (
    profiled
    .select(
        "id",
        "site_name",
        "installed_kw",
        "site_name_len",
        "installed_kw_bucket",
        "max_reading_voltage",
        "reading_count",
        F.round(F.col("installed_kw") * 0.25, 2).alias("installed_kw_adj"),
        F.round(F.col("max_reading_voltage") * 0.25, 2).alias("max_reading_voltage_adj"),
        F.concat_ws("|", F.col("tariff"), F.col("site_name")).alias("tariff_site_name_label"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("kwh").alias("max_reading_kwh"))
)

with_stats = (
    enriched.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.max_reading_kwh")
)

result = (
    with_stats
    .rollup("reading_count", "site_name_len")
    .agg(F.array_sort(F.collect_set("site_name")).alias("site_name_list"))
    .orderBy(
        F.col("reading_count").asc_nulls_last(),
        F.col("site_name_len").asc_nulls_last(),
    )
)
