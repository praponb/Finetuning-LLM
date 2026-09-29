from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter((F.col("tariff").isin("FLAT", "TOU")) & (F.col("rate") < 5.9))
    .select("tariff")
)

tariff_side_2 = (
    df_tariff_windows
    .filter((F.col("rate") > 3.7) & (F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp")))
    .select("tariff")
)

result = (
    tariff_side
    .subtract(tariff_side_2)
    .orderBy("tariff")
)
