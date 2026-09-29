from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 1.1, 2).alias("kwh_adj"),
    )
)

enriched = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.kwh_adj",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

enriched_site = (
    enriched.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z3"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.site_tariff",
        "s.site_zone",
        F.col("si.zone").alias("site_zone_2"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff_2"),
    )
)

result = (
    enriched_site
    .cube("site_id", "site_tariff_2")
    .agg(
        F.sum(F.when(F.col("site_zone") != "Z4", F.col("kwh"))).alias("kwh_site_zone_z4"),
    )
)
