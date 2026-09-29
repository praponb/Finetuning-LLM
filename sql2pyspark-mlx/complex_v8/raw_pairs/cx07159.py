from pyspark.sql import functions as F

filtered = (
    df_tariff_windows
    .filter((F.col("rate").between(2.2, 5.9)) & (F.to_date(F.col("valid_to")) < F.lit("2025-03-12").cast("date")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.round((F.unix_timestamp(F.col("valid_from")) - F.unix_timestamp(F.col("valid_to"))) / 3600, 1).alias("hours_valid_to_to_valid_from"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("valid_from")).alias("days_since_valid_from"),
        F.round((F.unix_timestamp(F.col("valid_from")) - F.unix_timestamp(F.col("valid_to"))) / 3600, 1).alias("hours_valid_to_to_valid_from_2"),
    )
)

screened = (
    filtered
    .crossJoin(filtered.agg(F.min("hours_valid_to_to_valid_from").alias("overall_min_hours_valid_to_to_valid_from")))
    .filter(F.col("hours_valid_to_to_valid_from") > F.col("overall_min_hours_valid_to_to_valid_from"))
    .drop("overall_min_hours_valid_to_to_valid_from")
)

result = (
    screened
    .select("id", "hours_valid_to_to_valid_from", "rate")
    .unpivot(
        ids=["id"],
        values=["hours_valid_to_to_valid_from", "rate"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
