from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(F.col("site_id").alias("ref_id"), "read_at", "voltage")
)

reading_side = df_meter_readings.select(F.col("id").alias("ref_id"), "read_at", "voltage")

reading_side_2 = (
    df_meter_readings
    .filter((F.col("quality").isin("OK", "EST")) & (F.col("site_id") <= 4))
    .select(F.col("site_id").alias("ref_id"), "read_at", "voltage")
)

result = (
    reading_rows
    .unionByName(reading_side)
    .unionByName(reading_side_2)
)
