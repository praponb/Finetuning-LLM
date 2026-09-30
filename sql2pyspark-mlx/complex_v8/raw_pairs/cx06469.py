from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-12 08:00:00").cast("timestamp"))
    .select(
        "id",
        F.col("site_id").alias("qty"),
        F.col("kwh").alias("value"),
        F.lit("reading").alias("source"),
    )
)

reading_side_2 = (
    df_meter_readings
    .filter((F.col("voltage") <= 238.3) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "kwh",
        F.lit("reading_2").alias("source"),
    )
)

result = reading_side.union(reading_side_2)
