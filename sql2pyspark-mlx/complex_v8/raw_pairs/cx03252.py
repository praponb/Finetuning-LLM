from pyspark.sql import functions as F

joined_ranges = (
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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

quality_stats = (
    joined_ranges
    .groupBy("quality")
    .agg(F.percentile_approx("rate", 0.5).alias("median_rate"))
)

compared = (
    joined_ranges.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.rate") >= F.col("g.median_rate"))
    .select(
        "s.*",
        "g.median_rate",
        F.round(F.col("s.rate") - F.col("g.median_rate"), 2).alias("rate_minus_median"),
    )
)

site_id_stats = (
    compared
    .groupBy("site_id")
    .agg(F.percentile_approx("rate_minus_median", 0.5).alias("median_rate_minus_median"))
)

compared_2 = (
    compared.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.rate_minus_median") >= F.col("g.median_rate_minus_median"))
    .select(
        "s.*",
        "g.median_rate_minus_median",
        F.round(F.col("s.rate_minus_median") - F.col("g.median_rate_minus_median"), 2).alias("rate_minus_median_minus_median"),
    )
)

result = (
    compared_2
    .select("id", "energy_cost", "rate")
    .unpivot(
        ids=["id"],
        values=["energy_cost", "rate"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
