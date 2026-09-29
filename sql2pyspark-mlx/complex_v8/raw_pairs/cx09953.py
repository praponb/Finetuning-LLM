from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter((F.col("voltage") <= 237.5) & (F.col("read_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")))
    .select(
        F.lit("reading").alias("origin"),
        F.col("site_id").alias("entity_id"),
        F.col("quality").alias("category"),
    )
)

tariff_side = df_tariff_windows.select(F.lit("tariff").alias("origin"), "id", "tariff")

result = reading_rows.union(tariff_side)
