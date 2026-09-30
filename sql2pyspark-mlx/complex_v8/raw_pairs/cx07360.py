from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter((F.col("product") != "HOME") | (F.col("status") == "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
)

summary = (
    scoped
    .groupBy(
        "holder_id",
        F.date_trunc("month", "start_date").alias("start_date_month"),
        "product",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("premium").alias("max_premium"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.min("premium").alias("min_policy_premium"))
)

with_counts = (
    summary.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.min_policy_premium")
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("end_date").alias("first_policy_end_date"),
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
    )
)

with_stats = (
    with_counts.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_end_date", "p.avg_policy_premium")
)

result = (
    with_stats
    .groupBy("start_date_month", "distinct_ids")
    .pivot("product", ["HOME", "AUTO", "HEALTH", "LIFE"])
    .agg(F.count("min_policy_premium"))
    .orderBy("start_date_month", "distinct_ids")
)
