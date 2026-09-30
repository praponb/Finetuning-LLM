from pyspark.sql import functions as F

cleaned = (
    df_tariff_windows
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.round(F.col("rate") * 1.5, 2).alias("rate_adj"),
        F.to_date(F.col("valid_to")).alias("valid_to_date"),
    )
)

stats = (
    cleaned
    .groupBy(
        F.year("valid_to_date").alias("valid_to_date_year"),
        F.date_trunc("month", "valid_from").alias("valid_from_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.min("rate").alias("min_rate"),
        F.min("valid_from").alias("first_valid_from"),
        F.max("valid_to").alias("last_valid_to"),
        F.round(F.avg("rate"), 2).alias("avg_rate"),
    )
)

result = (
    stats
    .select(
        "valid_to_date_year",
        "valid_from_month",
        "min_rate",
        "avg_rate",
    )
    .unpivot(
        ids=["valid_to_date_year", "valid_from_month"],
        values=["min_rate", "avg_rate"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
