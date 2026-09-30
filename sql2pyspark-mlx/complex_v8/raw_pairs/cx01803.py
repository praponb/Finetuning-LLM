from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-03-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

labeled = (
    scoped
    .select(
        "*",
        F.when(F.col("premium") >= 2945.9, "A").when(F.col("premium") >= 1541.8, "B").otherwise("C").alias("premium_tier"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.min("premium").alias("min_policy_premium"),
    )
)

summarized_rows = (
    labeled.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.min_policy_premium",
    )
)

result = (
    summarized_rows
    .select("id", "min_policy_premium", "premium")
    .unpivot(
        ids=["id"],
        values=["min_policy_premium", "premium"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
