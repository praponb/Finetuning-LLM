from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug_2"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

filtered = (
    with_parent.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") > 214.3), "left_semi")
)

site_zone_stats = (
    filtered
    .groupBy("site_zone")
    .agg(F.max("kwh").alias("max_kwh"))
)

outliers = (
    filtered.alias("s")
    .join(site_zone_stats.alias("g"), F.col("g.site_zone") == F.col("s.site_zone"), "inner")
    .filter(F.col("s.kwh") == F.col("g.max_kwh"))
    .select(
        "s.*",
        "g.max_kwh",
        F.round(F.col("s.kwh") - F.col("g.max_kwh"), 2).alias("kwh_minus_max"),
    )
)

rollup_base = (
    outliers
    .groupBy("site_id")
    .agg(
        F.max("max_kwh").alias("max_max_kwh"),
        F.datediff(F.max("read_at_date"), F.min("read_at_date")).alias("read_at_date_span_days"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("site_zone")).alias("site_zone_list"),
        F.sum("kwh_minus_max").alias("sum_kwh_minus_max_check"),
    )
    .filter(F.col("sum_kwh_minus_max_check") > -48.0)
    .drop("sum_kwh_minus_max_check")
)

result = (
    rollup_base
    .filter(F.col("max_max_kwh") <= 90.0)
    .select(
        "site_id",
        "site_zone_list",
        F.round(F.col("max_max_kwh") / F.nullif(F.col("n_rows"), F.lit(0)), 3).alias("max_max_kwh_per_n_rows"),
    )
    .orderBy(F.col("max_max_kwh_per_n_rows").asc(), "site_id")
)
