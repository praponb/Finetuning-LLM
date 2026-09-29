from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter(F.col("tariff").isin("FLAT", "TOU"))
    .select("id")
)

tariff_side_2 = df_tariff_windows.select("id")

result = (
    tariff_side
    .subtract(tariff_side_2)
    .orderBy(F.col("id").desc())
)
