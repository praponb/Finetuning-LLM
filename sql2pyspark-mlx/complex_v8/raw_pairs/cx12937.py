from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .filter(F.col("tariff").isin("FLAT", "EV"))
    .select("id", "valid_to", F.col("tariff").alias("category"))
)

tariff_side = (
    df_tariff_windows
    .filter((F.col("tariff").isNotNull()) & (F.col("rate") >= 2.2))
    .select("id", "valid_from", "tariff")
)

result = tariff_rows.union(tariff_side)
