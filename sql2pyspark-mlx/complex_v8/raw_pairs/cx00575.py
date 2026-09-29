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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

joined = (
    matched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

combined_details_site = (
    joined.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(~F.col("si.zone").isin("Z3", "Z2"))
    .select("s.*", F.col("si.zone").alias("site_zone_2"))
)

enriched = (
    combined_details_site
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "rate",
        "site_commissioned_on",
        "site_zone",
        "site_zone_2",
        F.least(F.col("energy_cost"), F.col("voltage")).alias("least_energy_cost_voltage"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

result = (
    enriched
    .filter(F.col("least_energy_cost_voltage") < 540.0)
    .select(
        "id",
        "voltage",
        "rate",
        "site_commissioned_on",
        "site_zone",
        "site_zone_2",
        "kwh_bucket",
    )
    .orderBy(
        F.col("site_commissioned_on").desc(),
        F.col("site_zone").desc_nulls_first(),
    )
)
