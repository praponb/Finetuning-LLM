from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("voltage") < 241.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_parent_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

with_parent = (
    with_parent_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.site_zone",
        "s.site_name",
        F.col("si.zone").alias("site_zone_2"),
    )
)

joined_site = (
    with_parent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.site_zone",
        "s.site_name",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone_3"),
    )
)

result = (
    joined_site
    .groupBy("site_id")
    .pivot("site_tariff", ["EV", "TOU", "FLAT"])
    .agg(F.count("site_installed_kw"))
)
