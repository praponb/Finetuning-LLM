from pyspark.sql import functions as F

reading_rows = df_meter_readings.select("id", F.col("quality").alias("category"))

reading_rows_2 = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select("site_id", "quality")
)

result = reading_rows.subtract(reading_rows_2)
