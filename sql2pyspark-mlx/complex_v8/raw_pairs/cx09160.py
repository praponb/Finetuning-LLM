from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("kwh") >= 52.5))
    .select("site_id", F.col("id").alias("key_id"), F.lit("reading").alias("origin"))
)

reading_side = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) & (F.hour(F.col("read_at")).between(9, 14)))
    .select(
        "site_id",
        F.col("id").alias("key_id"),
        F.lit("reading_2").alias("origin"),
    )
)

result = (
    reading_rows
    .union(reading_side)
    .orderBy("origin", "site_id")
)
