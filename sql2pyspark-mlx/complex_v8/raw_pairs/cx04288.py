from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("site_id") >= 4)
    .select(
        F.lit("reading").alias("record_type"),
        "site_id",
        F.col("voltage").alias("amount"),
    )
)

tariff_side = (
    df_tariff_windows
    .select(
        F.lit("tariff").alias("record_type"),
        F.col("id").alias("site_id"),
        F.col("rate").alias("amount"),
    )
)

result = (
    reading_side
    .union(tariff_side)
    .orderBy(F.col("amount").desc(), "record_type")
)
