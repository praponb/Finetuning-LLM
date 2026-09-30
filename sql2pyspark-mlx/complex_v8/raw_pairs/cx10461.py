from pyspark.sql import functions as F

base_tariffs = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_to")).between(8, 11)) & (F.col("rate") <= 7.6))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
    )
)

tariff_side = (
    base_tariffs
    .filter(F.col("valid_from") < F.lit("2025-02-18 04:00:00").cast("timestamp"))
    .select("id", F.col("tariff").alias("category"))
)

tariff_side_2 = (
    df_tariff_windows
    .filter((F.col("rate") > 4.2) & (~F.col("tariff").isin("FLAT", "EV")))
    .select("id", F.col("tariff").alias("category"))
)

result = (
    tariff_side
    .intersectAll(tariff_side_2)
    .orderBy("id", "category")
)
