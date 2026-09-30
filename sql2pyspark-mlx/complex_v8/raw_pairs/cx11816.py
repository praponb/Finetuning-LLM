from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .filter((F.month(F.col("valid_from")).isin(1, 3)) & (F.col("valid_to") < F.lit("2025-04-05 02:00:00").cast("timestamp")))
    .select(F.lit("tariff").alias("origin"), "id", "valid_from")
)

tariff_side = (
    df_tariff_windows
    .select(
        F.lit("tariff_2").alias("origin"),
        "id",
        F.col("valid_to").alias("valid_from"),
    )
)

result = tariff_rows.union(tariff_side)
