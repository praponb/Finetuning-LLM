from pyspark.sql import functions as F

base_tariffs = df_tariff_windows.filter(F.month(F.col("valid_from")).isin(1, 3))

tariff_side = base_tariffs.select("id", F.col("valid_from").alias("happened_at"))

tariff_side_2 = df_tariff_windows.select("id", "valid_from")

result = (
    tariff_side
    .union(tariff_side_2)
    .orderBy("id", "happened_at")
)
