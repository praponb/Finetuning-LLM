from pyspark.sql import functions as F

tariff_rows = df_tariff_windows.select("id", F.col("tariff").alias("label"), "valid_from")

reading_rows = df_meter_readings.select("id", "quality", "read_at")

result = (
    tariff_rows
    .union(reading_rows)
    .distinct()
    .orderBy("valid_from")
)
