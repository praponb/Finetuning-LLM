from pyspark.sql import functions as F

tariff_rows = df_tariff_windows.select("id")

tariff_rows_2 = (
    df_tariff_windows
    .filter((F.col("rate") >= 3.7) & (F.col("tariff") == "TOU"))
    .select("id")
)

result = tariff_rows.subtract(tariff_rows_2)
