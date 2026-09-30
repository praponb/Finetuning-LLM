from pyspark.sql import functions as F

filtered = (
    df_policyholders
    .filter((F.upper(F.trim(F.col("holder_name"))).like("E%")) | (F.col("risk_score") >= 0.3))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
    )
)

with_children = (
    filtered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.premium") <= 4670.9), "inner")
    .select(
        "s.id",
        "s.region",
        "s.risk_score",
        "s.joined_on",
        F.col("p.id").alias("policy_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
    )
)

with_parent = (
    with_children.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.region",
        "s.risk_score",
        "s.joined_on",
        "s.policy_id",
        "s.policy_premium",
        "s.policy_end_date",
        "s.policy_status",
        F.col("p.end_date").alias("policy_end_date_2"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("end_date").alias("first_policy_end_date"),
        F.count("*").alias("policy_count"),
    )
)

with_stats = (
    with_parent.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.first_policy_end_date",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"))
)

summarized_rows = (
    with_stats.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.avg_claim_claimed_amount")
)

result = (
    summarized_rows
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        "policy_id",
        "policy_premium",
        "policy_end_date",
        "policy_end_date_2",
        "policy_holder_id",
        "policy_count",
        "avg_claim_claimed_amount",
        F.date_add(F.col("policy_end_date_2"), 7).alias("policy_end_date_2_plus_7d"),
        F.lower(F.regexp_replace(F.trim(F.col("region")), " ", "_")).alias("region_slug"),
    )
    .orderBy(F.col("risk_score").desc(), "id", "policy_id")
    .limit(10)
)
