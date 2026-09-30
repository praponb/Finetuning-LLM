from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

joined = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") >= F.lit("2024-09-15").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

joined_site = (
    joined.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.site_tariff",
        F.col("si.tariff").alias("site_tariff_2"),
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    joined_site
    .filter(F.col("kwh") > 45.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "site_tariff",
        "site_tariff_2",
        "site_zone",
    )
    .orderBy(F.col("read_at").desc(), F.col("site_zone").desc_nulls_first())
)
