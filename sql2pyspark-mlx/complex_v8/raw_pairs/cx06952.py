from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_to")).between(9, 11)) & (F.col("tariff") == "TOU"))
    .select(F.col("tariff").alias("label"), "valid_to")
)

reading_rows = df_meter_readings.select("quality", "read_at")

result = tariff_side.union(reading_rows)
