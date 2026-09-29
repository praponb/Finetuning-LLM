from pyspark.sql import functions as F

base_tariffs = (
    df_tariff_windows
    .filter(F.col("tariff") == "FLAT")
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

tariff_side = (
    base_tariffs
    .select(
        "tariff_or_unknown",
        F.col("valid_from").alias("happened_at"),
        "rate",
        F.lit("tariff").alias("source"),
    )
)

tariff_rows = (
    df_tariff_windows
    .select(
        F.col("tariff").alias("tariff_or_unknown"),
        F.col("valid_from").alias("happened_at"),
        "rate",
        F.lit("tariff_2").alias("source"),
    )
)

result = (
    tariff_side
    .unionByName(tariff_rows)
    .distinct()
    .orderBy("happened_at")
)
