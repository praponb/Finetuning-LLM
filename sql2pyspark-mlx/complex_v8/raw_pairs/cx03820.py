from pyspark.sql import functions as F

filtered = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

grouped = (
    filtered
    .filter(F.col("product") == "HOME")
    .groupBy("status")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("days_since_end_date").alias("max_days_since_end_date"),
        F.round(F.sum("days_since_end_date") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("days_since_end_date_to_premium_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("product").alias("distinct_products"),
    )
)

with_activity = (
    grouped
    .crossJoin(grouped.agg(F.min("n_rows").alias("overall_min_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_min_n_rows"))
    .drop("overall_min_n_rows")
)

result = (
    with_activity
    .select("status", "distinct_products", "distinct_ids")
    .unpivot(
        ids=["status"],
        values=["distinct_products", "distinct_ids"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
