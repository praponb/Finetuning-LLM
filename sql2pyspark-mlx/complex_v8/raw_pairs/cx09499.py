from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter((F.col("rate").between(4.2, 7.6)) & (F.col("tariff").isin("EV", "FLAT")))
    .select(F.col("id").alias("key_id"))
)

reading_side = df_meter_readings.select("site_id")

result = (
    tariff_side
    .subtract(reading_side)
    .orderBy(F.col("key_id").desc())
)
