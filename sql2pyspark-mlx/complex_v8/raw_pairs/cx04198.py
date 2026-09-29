from pyspark.sql import functions as F

prepared_tariffs = (
    df_tariff_windows
    .filter((F.col("rate") >= 3.0) & (F.col("valid_from") < F.lit("2025-03-09 05:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        F.date_trunc("quarter", F.col("valid_to")).alias("valid_to_quarter"),
        F.substring(F.trim(F.col("tariff")), 1, 3).alias("tariff_prefix"),
    )
)

screened = (
    prepared_tariffs
    .crossJoin(prepared_tariffs.agg(F.max("rate").alias("overall_max_rate")))
    .filter(F.col("rate") < F.col("overall_max_rate"))
    .drop("overall_max_rate")
)

labeled = (
    screened
    .filter(F.month(F.col("valid_to_quarter")).isin(1, 2))
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("valid_to_quarter")) - F.unix_timestamp(F.col("valid_to"))) / 60, 1).alias("minutes_valid_to_to_valid_to_quarter"),
        F.round((F.unix_timestamp(F.col("valid_to")) - F.unix_timestamp(F.col("valid_to_quarter"))) / 3600, 1).alias("hours_valid_to_quarter_to_valid_to"),
        F.concat(F.col("tariff_prefix"), F.lit("#"), F.col("id").cast("string")).alias("tariff_prefix_ref"),
    )
)

result = (
    labeled
    .select(
        "id",
        F.col("rate").alias("rate_v"),
        F.col("hours_valid_to_quarter_to_valid_to").alias("m_hours_valid_to_quarter_to_valid_to"),
    )
    .unpivot(
        ids=["id"],
        values=["rate_v", "m_hours_valid_to_quarter_to_valid_to"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
