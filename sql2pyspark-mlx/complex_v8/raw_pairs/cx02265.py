from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter(~F.col("quality").isin("OK", "EST"))
    .select("id", F.col("site_id").alias("entity_id"), "read_at")
)

reading_rows_2 = df_meter_readings.select("id", F.col("site_id").alias("entity_id"), "read_at")

reading_side = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select("id", F.col("site_id").alias("entity_id"), "read_at")
)

result = (
    reading_rows
    .unionByName(reading_rows_2)
    .unionByName(reading_side)
    .orderBy("id", "read_at")
)
