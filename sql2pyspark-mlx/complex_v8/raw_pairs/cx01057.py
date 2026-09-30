from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") < 235.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

with_details_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_week",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

enriched = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").between(F.lit("2024-09-15").cast("date"), F.lit("2025-11-28").cast("date"))), "inner")
    .filter(F.col("si.installed_kw") <= 373.3)
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

result = (
    enriched
    .filter(F.hour(F.col("read_at")).between(7, 13))
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "site_commissioned_on",
    )
    .orderBy(F.col("site_commissioned_on").desc(), "id")
    .limit(5)
)
