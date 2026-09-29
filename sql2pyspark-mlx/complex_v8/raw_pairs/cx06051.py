from pyspark.sql import functions as F

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

combined_details_site = (
    joined_ranges.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") <= 284.0), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

zone_stats = (
    combined_details_site
    .groupBy("zone")
    .agg(F.percentile_approx("rate", 0.5).alias("median_rate"))
)

vs_group = (
    combined_details_site.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.rate") >= F.col("g.median_rate"))
    .select(
        "s.*",
        "g.median_rate",
        F.round(F.col("s.rate") - F.col("g.median_rate"), 2).alias("rate_minus_median"),
    )
)

result = (
    vs_group
    .groupingSets(
        [["site_tariff", "quality"], ["site_tariff"], ["quality"], []],
        "site_tariff", "quality",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.col("zone") == "Z4", 1)).alias("n_zone_z4"),
        F.sum(F.when(F.col("quality").isin("EST", "OK"), F.col("rate_minus_median"))).alias("rate_minus_median_quality_in_est_ok"),
    )
    .select(
        F.coalesce(F.col("site_tariff"), F.lit("ALL")).alias("site_tariff"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "grouping_level",
        "n_zone_z4",
        "rate_minus_median_quality_in_est_ok",
    )
)
