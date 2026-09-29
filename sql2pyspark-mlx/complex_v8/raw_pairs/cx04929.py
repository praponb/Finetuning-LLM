from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("kwh") > 33.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

filtered = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.tariff").isin("TOU", "EV")), "left_semi")
)

with_details = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_details
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "site_installed_kw",
        "site_tariff",
        F.concat_ws(" / ", F.col("quality"), F.col("site_tariff")).alias("quality_site_tariff_label"),
    )
    .orderBy(
        F.col("site_installed_kw").desc_nulls_first(),
        F.col("site_id").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
