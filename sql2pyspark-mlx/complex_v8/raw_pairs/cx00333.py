from pyspark.sql import functions as F

base = (
    df_policyholders
    .filter((F.col("segment") == "RETAIL") & (F.col("region") == "South"))
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)

rollup_base = (
    base
    .filter(F.col("region") != "West")
    .groupBy(
        "region",
        F.when(F.col("days_since_joined_on") >= 490, "large").when(F.col("days_since_joined_on") >= 140, "medium").otherwise("small").alias("days_since_joined_on_tier"),
        F.date_format("joined_on", "yyyy-MM").alias("joined_on_ym"),
    )
    .agg(
        F.percentile_approx("days_since_joined_on", 0.5).alias("median_days_since_joined_on"),
        F.datediff(F.max("joined_on"), F.min("joined_on")).alias("joined_on_span_days"),
        F.round(F.stddev("days_since_joined_on"), 2).alias("stddev_days_since_joined_on"),
    )
)

screened = (
    rollup_base
    .crossJoin(rollup_base.agg(F.max("stddev_days_since_joined_on").alias("overall_max_stddev_days_since_joined_on")))
    .filter(F.col("stddev_days_since_joined_on") < F.col("overall_max_stddev_days_since_joined_on"))
    .drop("overall_max_stddev_days_since_joined_on")
)

result = (
    screened
    .select(
        "region",
        "days_since_joined_on_tier",
        "joined_on_span_days",
        "median_days_since_joined_on",
    )
    .unpivot(
        ids=["region", "days_since_joined_on_tier"],
        values=["joined_on_span_days", "median_days_since_joined_on"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
