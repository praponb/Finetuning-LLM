from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select("site_id", F.col("quality").alias("name"))
)

reading_side = (
    df_meter_readings
    .filter(F.col("kwh").between(43.1, 81.3))
    .select(F.col("id").alias("site_id"), F.col("quality").alias("name"))
)

result = (
    reading_rows
    .subtract(reading_side)
    .orderBy("site_id", "name")
)
