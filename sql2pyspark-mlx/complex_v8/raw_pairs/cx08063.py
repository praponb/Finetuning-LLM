from pyspark.sql import functions as F

prepared = (
    df_tariff_windows
    .filter((F.col("rate") < 5.9) & (F.col("valid_from") >= F.lit("2025-01-23 19:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.substring(F.trim(F.col("tariff")), 1, 2).alias("tariff_prefix"),
        F.date_format(F.col("valid_from"), "yyyy-MM-dd").alias("valid_from_label"),
    )
)

tagged = (
    prepared
    .select(
        "*",
        F.upper(F.trim(F.col("tariff_prefix"))).alias("tariff_prefix_clean"),
    )
)

result = (
    tagged
    .filter(F.col("valid_from") >= F.lit("2025-01-26 03:00:00").cast("timestamp"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_from_label",
        "tariff_prefix_clean",
    )
    .orderBy("tariff_prefix_clean", "id")
    .limit(5)
)
