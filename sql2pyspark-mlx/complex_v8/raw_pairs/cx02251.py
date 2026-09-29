from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

summary = (
    scoped
    .groupBy("quality", "site_id")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.min("kwh").alias("min_kwh"),
        F.round(100 * F.count(F.when(~F.col("quality").isin("BAD", "OK"), 1)) / F.count("*"), 1).alias("pct_quality_not_in_bad_ok"),
        F.count("*").alias("n_rows"),
    )
)

without_matches = (
    summary.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isin("OK", "EST")) & (F.col("m.kwh").between(6.8, 117.1)), "left_semi")
)

combined_details_site = (
    without_matches.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.quality",
        "s.site_id",
        "s.min_kwh",
        "s.pct_quality_not_in_bad_ok",
        "s.n_rows",
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_details = (
    combined_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.quality", "s.site_id", F.col("si.zone").alias("site_zone"))
)

result = (
    with_details
    .select("quality", "site_id")
    .orderBy("site_id")
)
