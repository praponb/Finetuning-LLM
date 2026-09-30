from pyspark.sql import functions as F

tariff_side = df_tariff_windows.select(F.col("id").alias("key_id"))

reading_side = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(F.col("id").alias("key_id"))
)

result = (
    tariff_side
    .intersect(reading_side)
    .orderBy("key_id")
)
