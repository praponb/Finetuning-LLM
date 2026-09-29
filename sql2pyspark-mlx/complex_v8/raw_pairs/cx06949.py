from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter(F.col("kwh") <= 109.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

reading_side = scoped_readings.select(F.col("site_id").alias("key_id"), "id", "quality")

reading_side_2 = df_meter_readings.select("site_id", "id", "quality")

reading_side_3 = df_meter_readings.select("id", "site_id", "quality")

result = (
    reading_side
    .union(reading_side_2)
    .union(reading_side_3)
    .orderBy("quality", "id")
)
