from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.col("voltage") >= 222.3) & (F.col("kwh") < 90.6))
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
    prepared
    .filter((F.col("read_at") >= F.lit("2025-01-13 15:00:00").cast("timestamp")) & (F.col("voltage") <= 238.3))
    .select(F.col("quality").alias("label"))
)

tariff_rows = (
    df_tariff_windows
    .filter(F.col("tariff") != "TOU")
    .select("tariff")
)

result = (
    reading_side
    .subtract(tariff_rows)
    .orderBy("label")
)
