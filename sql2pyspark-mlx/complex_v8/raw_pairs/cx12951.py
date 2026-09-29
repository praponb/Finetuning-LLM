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

with_details_site = (
    joined_ranges.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_details_site
    .cube("zone", "quality", "site_id")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("rate"), 2).alias("total_rate"),
        F.round(F.sum("kwh") / F.nullif(F.sum("rate"), F.lit(0)), 3).alias("kwh_to_rate_ratio"),
    )
    .orderBy(
        F.col("zone").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
