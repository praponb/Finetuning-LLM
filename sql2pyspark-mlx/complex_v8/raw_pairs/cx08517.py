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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

combined_details_site = (
    in_window.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff") == "TOU")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        F.col("si.tariff").alias("site_tariff"),
    )
)

joined_site = (
    combined_details_site.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw").between(66.5, 386.6)), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        "s.site_tariff",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    joined_site
    .select(
        "id",
        "quality",
        "rate",
        "energy_cost",
        "site_tariff",
        "site_name",
        F.round(F.col("rate") / F.nullif(F.col("energy_cost"), F.lit(0)), 3).alias("rate_per_energy_cost"),
        F.concat(F.col("site_zone"), F.lit("#"), F.col("id").cast("string")).alias("site_zone_ref"),
    )
    .orderBy(F.col("energy_cost").desc())
)
