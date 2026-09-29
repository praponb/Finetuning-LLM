from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("policy_id") >= 16) & (F.col("claim_type").isin("flood", "fire", "delay")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

with_details_policy = (
    filtered_2.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status") == "ACTIVE"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status").isin("IN_REVIEW", "REJECTED"), 1)).alias("claim_status_count"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

profiled = (
    with_details_policy.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.first_claim_date",
    )
)

result = (
    profiled
    .select("id", "policy_premium", "claimed_amount")
    .unpivot(
        ids=["id"],
        values=["policy_premium", "claimed_amount"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
