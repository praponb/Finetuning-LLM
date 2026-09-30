from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter((F.col("kwh") <= 109.3) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_side = (
    cleaned_readings
    .filter((F.col("quality").isNotNull()) & (F.col("voltage") > 216.5))
    .select("quality", "kwh", F.lit("reading").alias("source"))
)

tariff_side = (
    df_tariff_windows
    .select(
        F.col("tariff").alias("quality"),
        F.col("rate").alias("kwh"),
        F.lit("tariff").alias("source"),
    )
)

site_side = (
    df_sites
    .filter(F.col("zone").isNotNull())
    .select(
        F.col("site_name").alias("quality"),
        F.col("installed_kw").alias("kwh"),
        F.lit("site").alias("source"),
    )
)

result = (
    reading_side
    .unionByName(tariff_side)
    .unionByName(site_side)
    .orderBy(F.col("quality").desc(), "source")
)
