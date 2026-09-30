from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("voltage") <= 233.1)
    .select(F.col("quality").alias("label"), "voltage")
)

reading_rows = df_meter_readings.select("quality", "voltage")

result = (
    reading_side
    .union(reading_rows)
    .orderBy("label")
)
