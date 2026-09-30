from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .filter(F.month(F.col("valid_from")).isin(1, 3))
    .select(F.col("id").alias("qty"))
)

tariff_rows_2 = df_tariff_windows.select("id")

result = tariff_rows.subtract(tariff_rows_2)
