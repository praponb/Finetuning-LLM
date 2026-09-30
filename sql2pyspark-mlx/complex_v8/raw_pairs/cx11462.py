from pyspark.sql import functions as F

matched = (
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

qualified = (
    matched.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 66.5) & (F.col("si.tariff") == "TOU"), "left_anti")
)

annotated = (
    qualified
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.col("energy_cost"), F.lit(0)), 3).alias("kwh_per_energy_cost"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

zone_stats = (
    annotated
    .groupBy("zone")
    .agg(F.percentile_approx("rate", 0.5).alias("median_rate"))
)

outliers = (
    annotated.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.rate") >= F.col("g.median_rate"))
    .select(
        "s.*",
        "g.median_rate",
        F.round(F.col("s.rate") - F.col("g.median_rate"), 2).alias("rate_minus_median"),
    )
)

result = (
    outliers
    .filter(F.col("quality_or_unknown") != "BAD")
    .select(
        "id",
        "read_at",
        "quality",
        "rate",
        "energy_cost",
        "kwh_per_energy_cost",
        "quality_or_unknown",
        "median_rate",
        F.lower(F.trim(F.col("quality_or_unknown"))).alias("quality_or_unknown_clean"),
        F.when(F.col("kwh_per_energy_cost") >= 6.5, "A").when(F.col("kwh_per_energy_cost") >= 3.0, "B").otherwise("C").alias("kwh_per_energy_cost_tier"),
    )
    .orderBy(F.col("quality_or_unknown").desc(), F.col("energy_cost").asc())
)
