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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

combined_details = (
    covered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_parent_site = (
    combined_details.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff") == "EV")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.rate",
        "s.energy_cost",
        "s.site_installed_kw",
        "s.site_zone",
        "s.site_tariff",
        "si.site_name",
        F.col("si.tariff").alias("site_tariff_2"),
    )
)

with_details = (
    with_parent_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.energy_cost",
        "s.site_installed_kw",
        "s.site_zone",
        "s.site_tariff",
        "s.site_tariff_2",
        F.col("si.zone").alias("site_zone_2"),
        F.col("si.tariff").alias("site_tariff_3"),
    )
)

result = (
    with_details
    .select("energy_cost", "site_tariff_2")
    .distinct()
    .orderBy("energy_cost")
)
