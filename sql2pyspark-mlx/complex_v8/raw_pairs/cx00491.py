from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter(F.col("valid_to") >= F.lit("2025-01-25 21:00:00").cast("timestamp"))
    .select(F.col("tariff").alias("name"), "id", F.lit("tariff").alias("origin"))
)

tariff_side_2 = (
    df_tariff_windows
    .select(F.col("tariff").alias("name"), "id", F.lit("tariff_2").alias("origin"))
)

result = (
    tariff_side
    .unionByName(tariff_side_2)
    .distinct()
    .orderBy(F.col("origin").desc())
)
