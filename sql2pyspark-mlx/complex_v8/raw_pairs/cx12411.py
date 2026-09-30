from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter(F.col("rate").between(2.2, 5.9))
    .select(
        F.col("id").alias("key_id"),
        F.col("tariff").alias("label"),
        "valid_to",
        F.lit("tariff").alias("origin"),
    )
)

reading_rows = (
    df_meter_readings
    .filter((F.col("site_id") >= 8) & (F.col("voltage") <= 237.8))
    .select(
        "id",
        "quality",
        "read_at",
        F.lit("reading").alias("origin"),
    )
)

result = tariff_side.union(reading_rows)
