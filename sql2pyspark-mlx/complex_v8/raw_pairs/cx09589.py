from pyspark.sql import functions as F

reading_side = df_meter_readings.select(F.col("site_id").alias("qty"), F.col("quality").alias("name"))

reading_rows = df_meter_readings.select("site_id", "quality")

result = (
    reading_side
    .union(reading_rows)
    .orderBy(F.col("qty").desc(), "name")
)
