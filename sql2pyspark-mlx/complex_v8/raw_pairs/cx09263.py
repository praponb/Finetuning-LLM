from pyspark.sql import functions as F

filtered = (
    df_tariff_windows
    .filter(F.to_date(F.col("valid_to")) < F.lit("2025-04-05").cast("date"))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
    )
)

tariff_rows = (
    filtered
    .filter(F.col("rate").between(2.2, 5.7))
    .select("tariff")
)

reading_side = (
    df_meter_readings
    .filter(F.col("kwh") <= 79.4)
    .select(F.col("quality").alias("tariff"))
)

result = (
    tariff_rows
    .intersect(reading_side)
    .orderBy("tariff")
)
