from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.date_add(F.col("end_date"), 90).alias("end_date_plus_90d"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.end_date_plus_90d",
        F.col("p.region").alias("holder_region"),
    )
)

enriched = (
    joined.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status",
        "s.end_date_plus_90d",
        "p.holder_name",
        F.col("p.region").alias("holder_region_2"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type") == "flood"), "left_anti")
)

scored = (
    qualified
    .select(
        "*",
        F.date_trunc("quarter", F.col("end_date_plus_90d")).alias("end_date_plus_90d_quarter"),
        F.date_add(F.col("holder_joined_on"), 90).alias("holder_joined_on_plus_90d"),
        F.lower(F.regexp_replace(F.trim(F.col("holder_region_2")), " ", "_")).alias("holder_region_2_slug"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.sum("premium").alias("total_policy_premium"),
        F.count("*").alias("policy_count"),
        F.count("*").alias("policy_count_2"),
    )
)

with_counts = (
    scored.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.total_policy_premium",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "holder_id",
        "product",
        "status",
        "holder_joined_on",
        "end_date_plus_90d_quarter",
        "holder_joined_on_plus_90d",
        "holder_region_2_slug",
        "total_policy_premium",
        "policy_count_2",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("holder_joined_on")).alias("days_since_holder_joined_on"),
    )
    .orderBy(F.col("policy_count_2").desc())
)
