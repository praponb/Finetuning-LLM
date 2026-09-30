from pyspark.sql import functions as F

scoped_tariffs = (
    df_tariff_windows
    .filter(F.col("tariff") == "EV")
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.lower(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

derived = (
    scoped_tariffs
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_clean",
        F.round(F.col("rate") * 1.1, 2).alias("rate_adj"),
    )
)

result = (
    derived
    .select("id", "tariff", "valid_from")
    .orderBy("tariff", F.col("id").asc_nulls_last())
    .limit(10)
)
