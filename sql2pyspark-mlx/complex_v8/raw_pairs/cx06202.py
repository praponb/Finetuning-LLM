from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-02 11:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

reading_side = prepared.select(F.col("quality").alias("name"))

site_side = (
    df_sites
    .filter((F.col("installed_kw").between(66.5, 284.0)) & (F.col("commissioned_on") >= F.lit("2024-10-28").cast("date")))
    .select("tariff")
)

result = (
    reading_side
    .subtract(site_side)
    .orderBy(F.col("name").desc())
)
