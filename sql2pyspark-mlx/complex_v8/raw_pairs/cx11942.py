from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter(F.hour(F.col("valid_to")).between(10, 14))
    .select(
        "id",
        F.col("tariff").alias("label"),
        F.col("valid_to").alias("event_time"),
    )
)

reading_side = (
    df_meter_readings
    .filter((~F.col("quality").isin("BAD", "EST")) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        F.col("quality").alias("label"),
        F.col("read_at").alias("event_time"),
    )
)

result = tariff_side.unionByName(reading_side)
