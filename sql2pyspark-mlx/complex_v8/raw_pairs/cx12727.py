from pyspark.sql import functions as F

prepared_tariffs = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_from")).between(6, 10)) & (F.col("tariff") == "TOU"))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
    )
)

tariff_rows = (
    prepared_tariffs
    .filter(F.col("rate") <= 5.9)
    .select(F.col("id").alias("ref_id"))
)

tariff_rows_2 = df_tariff_windows.select(F.col("id").alias("ref_id"))

result = (
    tariff_rows
    .subtract(tariff_rows_2)
    .orderBy("ref_id")
)
