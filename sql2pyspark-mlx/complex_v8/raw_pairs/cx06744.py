from pyspark.sql import functions as F

reading_side = df_meter_readings.select("site_id", F.col("read_at").alias("event_time"))

reading_rows = df_meter_readings.select("site_id", F.col("read_at").alias("event_time"))

result = (
    reading_side
    .unionByName(reading_rows)
    .orderBy(F.col("event_time").desc(), "site_id")
)
