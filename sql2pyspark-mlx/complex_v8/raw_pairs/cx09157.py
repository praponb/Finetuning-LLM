from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("holder_id") <= 17)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

joined = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") > 0.2)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.status_or_unknown",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.region").alias("holder_region"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("premium").alias("min_policy_premium"),
        F.count(F.when(F.col("status").isin("CANCELLED", "ACTIVE"), 1)).alias("policy_status_count"),
    )
)

with_counts = (
    joined.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.min_policy_premium",
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
    )
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(F.min("end_date").alias("first_policy_end_date"))
)

profiled = (
    with_counts.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_end_date")
)

result = (
    profiled
    .cube("product", "holder_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("min_policy_premium"), 2).alias("avg_min_policy_premium"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "holder_id",
        "gid",
        "avg_min_policy_premium",
    )
)
