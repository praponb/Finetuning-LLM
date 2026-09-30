from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .select(F.col("id").alias("qty"), F.col("valid_from").alias("happened_at"))
)

tariff_side_2 = df_tariff_windows.select("id", "valid_to")

result = tariff_side.union(tariff_side_2)
