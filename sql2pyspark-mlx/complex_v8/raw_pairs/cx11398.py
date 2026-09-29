from pyspark.sql import functions as F

tariff_side = df_tariff_windows.select(F.col("id").alias("ref_id"))

tariff_side_2 = (
    df_tariff_windows
    .filter(F.col("rate").between(2.2, 6.1))
    .select(F.col("id").alias("ref_id"))
)

result = (
    tariff_side
    .intersect(tariff_side_2)
    .orderBy("ref_id")
)
