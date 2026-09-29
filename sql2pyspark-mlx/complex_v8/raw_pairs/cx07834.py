from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(9, 15)) & (~F.col("quality").isin("EST", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

reading_side = (
    scoped
    .filter((F.hour(F.col("read_at")).between(10, 12)) & (~F.col("quality").isin("OK", "EST")))
    .select("id", "quality")
)

reading_side_2 = df_meter_readings.select("id", "quality")

result = (
    reading_side
    .union(reading_side_2)
    .distinct()
    .orderBy("quality", "id")
)
