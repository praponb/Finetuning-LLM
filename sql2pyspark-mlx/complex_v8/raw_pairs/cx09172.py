from pyspark.sql import functions as F

reading_side = df_meter_readings.select("site_id")

reading_rows = df_meter_readings.select("id")

result = (
    reading_side
    .exceptAll(reading_rows)
    .orderBy(F.col("site_id").desc())
)
