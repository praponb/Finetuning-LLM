from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter(F.col("valid_from") >= F.lit("2025-02-15 04:00:00").cast("timestamp"))
    .select("id")
)

tariff_side_2 = (
    df_tariff_windows
    .filter((F.col("rate").between(2.0, 7.6)) & (F.month(F.col("valid_from")).isin(1, 2)))
    .select("id")
)

result = tariff_side.intersect(tariff_side_2)
