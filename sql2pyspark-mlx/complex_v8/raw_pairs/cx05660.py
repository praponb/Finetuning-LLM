from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .select(F.col("id").alias("ref_id"), F.col("valid_from").alias("event_time"))
)

reading_side = (
    df_meter_readings
    .filter((F.col("quality") != "OK") & (F.col("voltage").between(218.5, 237.5)))
    .select("id", "read_at")
)

result = (
    tariff_side
    .union(reading_side)
    .orderBy("event_time", "ref_id")
)
