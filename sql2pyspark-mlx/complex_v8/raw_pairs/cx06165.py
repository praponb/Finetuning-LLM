from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .select(
        "tariff",
        "id",
        F.col("rate").alias("value"),
        F.lit("tariff").alias("source"),
    )
)

tariff_rows_2 = (
    df_tariff_windows
    .select(
        "tariff",
        "id",
        "rate",
        F.lit("tariff_2").alias("source"),
    )
)

result = (
    tariff_rows
    .union(tariff_rows_2)
    .orderBy(F.col("source").desc())
)
