from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("voltage") < 238.8)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

with_details_site = (
    base.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") >= 93.3), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

result = (
    with_details_site
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "site_zone",
        "site_tariff",
        "site_name",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
    .orderBy("voltage", F.col("voltage_per_kwh").asc())
)
