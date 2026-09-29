from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter(F.col("kwh") <= 109.3)
    .select("site_id", "quality")
)

reading_side = df_meter_readings.select("site_id", "quality")

result = (
    reading_rows
    .subtract(reading_side)
    .orderBy(F.col("site_id").desc(), "quality")
)
