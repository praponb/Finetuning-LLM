from pyspark.sql import functions as F

reading_rows = df_meter_readings.select("id")

reading_side = (
    df_meter_readings
    .filter(F.col("kwh") < 113.0)
    .select("id")
)

result = (
    reading_rows
    .subtract(reading_side)
    .orderBy(F.col("id").desc())
)
