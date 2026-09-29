from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .select(
        F.col("tariff").alias("category"),
        F.col("valid_to").alias("event_time"),
        F.lit("tariff").alias("origin"),
    )
)

reading_rows = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        F.col("quality").alias("category"),
        F.col("read_at").alias("event_time"),
        F.lit("reading").alias("origin"),
    )
)

result = (
    tariff_side
    .unionByName(reading_rows)
    .orderBy("origin")
)
