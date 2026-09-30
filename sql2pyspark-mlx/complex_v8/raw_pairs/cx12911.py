from pyspark.sql import functions as F

recent_tariffs = (
    df_tariff_windows
    .filter(F.hour(F.col("valid_to")).between(10, 14))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
        F.substring(F.trim(F.col("tariff")), 1, 4).alias("tariff_prefix"),
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
    )
)

enriched = (
    recent_tariffs
    .select(
        "id",
        "valid_to",
        "rate",
        "tariff_prefix",
        "tariff_ref",
        (F.col("days_since_valid_to") + F.col("rate")).alias("days_since_valid_to_plus_rate"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

result = (
    enriched
    .select(
        "id",
        F.col("rate").alias("rate_v"),
        F.col("days_since_valid_to_plus_rate").alias("days_since_valid_to_plus_rate_v"),
    )
    .unpivot(
        ids=["id"],
        values=["rate_v", "days_since_valid_to_plus_rate_v"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
