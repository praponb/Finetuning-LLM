from pyspark.sql import functions as F

tariff_rows = df_tariff_windows.select(F.col("tariff").alias("category"), "valid_from")

tariff_rows_2 = (
    df_tariff_windows
    .select(
        F.col("tariff").alias("category"),
        F.col("valid_to").alias("valid_from"),
    )
)

tariff_side = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_to")).between(10, 14)) & (F.col("valid_from") >= F.lit("2025-01-26 03:00:00").cast("timestamp")))
    .select(F.col("tariff").alias("category"), "valid_from")
)

stacked = (
    tariff_rows
    .union(tariff_rows_2)
    .union(tariff_side)
)

result = (
    stacked
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("valid_from").alias("latest_valid_from"),
    )
    .orderBy("category")
)
