from pyspark.sql import functions as F

reading_side = df_meter_readings.select(F.col("id").alias("key_id"), "site_id", "kwh")

reading_rows = df_meter_readings.select(F.col("id").alias("key_id"), "site_id", "kwh")

result = (
    reading_side
    .unionByName(reading_rows)
    .distinct()
    .orderBy(F.col("site_id").desc())
)
