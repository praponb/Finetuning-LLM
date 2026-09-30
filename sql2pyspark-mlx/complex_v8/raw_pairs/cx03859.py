from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .select("site_id", "quality")
)

reading_side_2 = df_meter_readings.select("site_id", "quality")

result = reading_side.intersect(reading_side_2)
