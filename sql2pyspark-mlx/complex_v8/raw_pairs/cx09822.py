from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .select(F.col("id").alias("ref_id"), F.col("valid_to").alias("happened_at"))
)

tariff_side_2 = (
    df_tariff_windows
    .select(F.col("id").alias("ref_id"), F.col("valid_to").alias("happened_at"))
)

result = tariff_side.union(tariff_side_2)
