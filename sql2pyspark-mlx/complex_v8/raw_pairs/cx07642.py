from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter(F.col("voltage") > 215.6)
    .select("id", "quality")
)

reading_rows_2 = df_meter_readings.select(F.col("site_id").alias("id"), "quality")

result = (
    reading_rows
    .subtract(reading_rows_2)
    .orderBy(F.col("id").desc())
)
