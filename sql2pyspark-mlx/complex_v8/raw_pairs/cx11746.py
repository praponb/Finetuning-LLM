from pyspark.sql import functions as F

covered = (
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

labeled = (
    covered
    .select(
        "*",
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.initcap(F.trim(F.col("zone"))).alias("zone_clean"),
        F.round(F.col("energy_cost") * 1.5, 2).alias("energy_cost_adj"),
    )
)

joined_site = (
    labeled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        "s.quality_group",
        "s.zone_clean",
        "s.energy_cost_adj",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    joined_site
    .cube("site_id", "quality_group", "zone")
    .agg(
        F.min("site_commissioned_on").alias("first_site_commissioned_on"),
        F.sum("kwh").alias("total_kwh"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality_group").asc_nulls_last(),
        F.col("zone").asc_nulls_last(),
    )
)
