from pyspark.sql import functions as F

in_window = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

site_id_stats = (
    in_window
    .groupBy("site_id")
    .agg(F.percentile_approx("energy_cost", 0.5).alias("median_energy_cost"))
)

outliers = (
    in_window.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.energy_cost") >= F.col("g.median_energy_cost"))
    .select(
        "s.*",
        "g.median_energy_cost",
        F.round(F.col("s.energy_cost") - F.col("g.median_energy_cost"), 2).alias("energy_cost_minus_median"),
    )
)

screened = (
    outliers
    .crossJoin(outliers.agg(F.min("energy_cost").alias("overall_min_energy_cost")))
    .filter(F.col("energy_cost") > F.col("overall_min_energy_cost"))
    .drop("overall_min_energy_cost")
)

grouped = (
    screened
    .groupBy(
        F.when(F.col("rate") >= 6.1, "A").when(F.col("rate") >= 4.2, "B").otherwise("C").alias("rate_tier"),
        "zone",
        "site_id",
    )
    .agg(
        F.countDistinct("zone").alias("distinct_zones"),
        F.max("kwh").alias("max_kwh"),
        F.max("rate").alias("max_rate_check"),
    )
    .filter(F.col("max_rate_check") > 3.7)
    .drop("max_rate_check")
)

result = (
    grouped
    .select(
        "rate_tier",
        "site_id",
        "distinct_zones",
        "max_kwh",
        F.upper(F.trim(F.col("rate_tier"))).alias("rate_tier_clean"),
    )
    .orderBy(F.col("rate_tier_clean").desc(), F.col("site_id").desc_nulls_first())
)
