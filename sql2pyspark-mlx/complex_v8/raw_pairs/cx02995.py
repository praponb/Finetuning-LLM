from pyspark.sql import functions as F

reading_side = df_meter_readings.select("id", "site_id")

reading_side_2 = (
    df_meter_readings
    .filter((F.col("voltage") >= 223.0) & (~F.col("quality").isin("BAD", "OK")))
    .select("site_id", "id")
)

result = reading_side.union(reading_side_2)
