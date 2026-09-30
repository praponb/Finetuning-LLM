from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("quality") != "OK")
    .select("id", F.col("site_id").alias("key_id"), "voltage")
)

reading_side_2 = df_meter_readings.select("id", F.col("site_id").alias("key_id"), "voltage")

result = reading_side.unionByName(reading_side_2)
