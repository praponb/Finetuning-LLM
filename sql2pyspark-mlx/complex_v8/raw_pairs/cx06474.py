from pyspark.sql import functions as F

reading_rows = df_meter_readings.select("site_id", "id")

reading_side = df_meter_readings.select("site_id", "id")

reading_side_2 = df_meter_readings.select("id", "site_id")

result = (
    reading_rows
    .union(reading_side)
    .union(reading_side_2)
)
