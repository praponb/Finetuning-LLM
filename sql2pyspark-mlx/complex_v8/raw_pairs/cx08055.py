from pyspark.sql import functions as F

reading_side = df_meter_readings.select("id", "kwh", "site_id")

reading_side_2 = (
    df_meter_readings
    .filter(F.col("site_id") <= 5)
    .select("id", "kwh", "site_id")
)

result = (
    reading_side
    .union(reading_side_2)
    .orderBy(F.col("site_id").desc(), "kwh")
)
