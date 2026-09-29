from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .filter(F.to_date(F.col("valid_from")) < F.lit("2025-02-18").cast("date"))
    .select("id", "rate", F.lit("tariff").alias("record_type"))
)

tariff_side = (
    df_tariff_windows
    .filter((F.col("rate").between(3.0, 5.7)) & (F.col("valid_from") < F.lit("2025-02-18 04:00:00").cast("timestamp")))
    .select("id", "rate", F.lit("tariff_2").alias("record_type"))
)

result = (
    tariff_rows
    .unionByName(tariff_side)
    .distinct()
)
