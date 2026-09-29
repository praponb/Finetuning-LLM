from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter((F.col("voltage") > 221.4) & (F.col("quality").isin("EST", "OK")))
    .select(
        F.lit("reading").alias("source"),
        F.col("id").alias("entity_id"),
        "read_at",
        "quality",
    )
)

reading_side_2 = (
    df_meter_readings
    .select(
        F.lit("reading_2").alias("source"),
        "id",
        "read_at",
        "quality",
    )
)

result = (
    reading_side
    .union(reading_side_2)
    .orderBy("source")
)
