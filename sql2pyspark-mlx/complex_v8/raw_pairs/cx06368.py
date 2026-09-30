from pyspark.sql import functions as F

prepared = (
    df_tariff_windows
    .filter((F.month(F.col("valid_from")).isin(1, 3)) & (F.hour(F.col("valid_to")).between(10, 12)))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown_2"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("valid_from")).alias("days_since_valid_from"),
    )
)

filtered = (
    prepared
    .crossJoin(prepared.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

with_activity = (
    filtered
    .crossJoin(filtered.agg(F.min("rate").alias("overall_min_rate")))
    .filter(F.col("rate") > F.col("overall_min_rate"))
    .drop("overall_min_rate")
)

scored = (
    with_activity
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("valid_to")) - F.unix_timestamp(F.col("valid_from"))) / 3600, 1).alias("hours_valid_from_to_valid_to"),
        F.when(F.col("days_since_valid_from") >= 420, "HIGH").when(F.col("days_since_valid_from") >= 175, "MEDIUM").otherwise("LOW").alias("days_since_valid_from_tier"),
        F.concat(F.col("tariff_or_unknown_2"), F.lit("#"), F.col("id").cast("string")).alias("tariff_or_unknown_2_ref"),
    )
)

result = (
    scored
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "tariff_or_unknown",
        "tariff_or_unknown_2",
        "days_since_valid_from",
        "days_since_valid_from_tier",
        "tariff_or_unknown_2_ref",
        F.coalesce(F.col("tariff_or_unknown"), F.lit("UNKNOWN")).alias("tariff_or_unknown_or_unknown"),
        F.round((F.unix_timestamp(F.col("valid_from")) - F.unix_timestamp(F.col("valid_to"))) / 3600, 1).alias("hours_valid_to_to_valid_from"),
    )
    .orderBy(F.col("id").desc())
)
