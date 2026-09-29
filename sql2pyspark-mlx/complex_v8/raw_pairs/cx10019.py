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

enriched = (
    joined_ranges
    .select(
        "*",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.round(F.col("kwh") * 1.07, 2).alias("kwh_adj"),
    )
)

screened = (
    enriched
    .crossJoin(enriched.agg(F.min("kwh").alias("overall_min_kwh")))
    .filter(F.col("kwh") > F.col("overall_min_kwh"))
    .drop("overall_min_kwh")
)

result = (
    screened
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "energy_cost",
        "kwh_bucket",
        "quality_ref",
        "kwh_adj",
        F.lower(F.trim(F.col("zone"))).alias("zone_clean"),
        F.round(F.col("rate") * 1.1, 2).alias("rate_adj"),
    )
    .orderBy(F.col("id").desc(), F.col("read_at").desc())
)
