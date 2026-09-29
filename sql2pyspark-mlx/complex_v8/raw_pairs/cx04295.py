from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("site_id") >= 2)
    .select("quality", "voltage", F.lit("reading").alias("source"))
)

tariff_side = (
    df_tariff_windows
    .filter(F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp"))
    .select(
        F.col("tariff").alias("quality"),
        F.col("rate").alias("voltage"),
        F.lit("tariff").alias("source"),
    )
)

result = reading_side.union(tariff_side)
