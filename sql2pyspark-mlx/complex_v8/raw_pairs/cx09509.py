from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter((F.col("quality").isin("OK", "BAD")) & (F.col("voltage") <= 235.8))
    .select(
        F.col("site_id").alias("qty"),
        "read_at",
        "voltage",
        F.lit("reading").alias("src"),
    )
)

tariff_side = (
    df_tariff_windows
    .select(
        F.col("id").alias("qty"),
        F.col("valid_from").alias("read_at"),
        F.col("rate").alias("voltage"),
        F.lit("tariff").alias("src"),
    )
)

reading_side_2 = (
    df_meter_readings
    .filter((F.col("quality") == "OK") & (F.col("site_id") <= 9))
    .select(
        F.col("id").alias("qty"),
        "read_at",
        "voltage",
        F.lit("reading_3").alias("src"),
    )
)

result = (
    reading_side
    .union(tariff_side)
    .union(reading_side_2)
)
