from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("product") == "TRAVEL")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(F.col("product") == "HEALTH", 1)).alias("policy_product_count"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
    )
)

result = (
    summarized_rows
    .select("id", "premium_bucket", "policy_product_count")
    .unpivot(
        ids=["id"],
        values=["premium_bucket", "policy_product_count"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
