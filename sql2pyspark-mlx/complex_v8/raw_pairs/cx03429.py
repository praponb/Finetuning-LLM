from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter((F.col("valid_to") < F.lit("2025-03-09 04:00:00").cast("timestamp")) & (F.col("valid_from") < F.lit("2025-03-09 05:00:00").cast("timestamp")))
    .select("id", "rate")
)

tariff_side_2 = (
    df_tariff_windows
    .filter(F.hour(F.col("valid_from")).between(7, 13))
    .select("id", "rate")
)

result = tariff_side.union(tariff_side_2)
