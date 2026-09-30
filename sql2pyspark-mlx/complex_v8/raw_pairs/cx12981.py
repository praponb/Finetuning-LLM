from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .select(
        F.lit("reading").alias("record_type"),
        "quality",
        "voltage",
        F.col("read_at").alias("event_time"),
    )
)

reading_side = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(
        F.lit("reading_2").alias("record_type"),
        "quality",
        "voltage",
        "read_at",
    )
)

result = (
    reading_rows
    .union(reading_side)
    .orderBy(F.col("event_time").desc())
)
