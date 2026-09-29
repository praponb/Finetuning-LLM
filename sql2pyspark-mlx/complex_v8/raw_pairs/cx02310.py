from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .select(
        F.col("site_id").alias("qty"),
        "read_at",
        F.lit("reading").alias("origin"),
    )
)

tariff_side = df_tariff_windows.select("id", "valid_from", F.lit("tariff").alias("origin"))

result = (
    reading_side
    .union(tariff_side)
    .distinct()
    .orderBy(F.col("qty").desc(), "origin")
)
