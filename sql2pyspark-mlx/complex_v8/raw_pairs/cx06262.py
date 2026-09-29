from pyspark.sql import functions as F

cleaned_tariffs = (
    df_tariff_windows
    .filter((F.col("tariff") == "TOU") & (F.col("valid_from") < F.lit("2025-02-18 04:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.round(F.col("rate") * 1.1, 2).alias("rate_adj"),
    )
)

without_matches = (
    cleaned_tariffs
    .crossJoin(cleaned_tariffs.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

screened = (
    without_matches
    .crossJoin(without_matches.agg(F.min("rate").alias("overall_min_rate")))
    .filter(F.col("rate") > F.col("overall_min_rate"))
    .drop("overall_min_rate")
)

enriched = (
    screened
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "rate_adj",
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
    )
)

result = (
    enriched
    .filter(F.col("rate").between(3.0, 7.6))
    .select(
        "id",
        "valid_to",
        "rate",
        "rate_adj",
        "rate_filled",
        F.to_date(F.col("valid_from")).alias("valid_from_date"),
    )
    .orderBy(F.col("rate").asc())
)
