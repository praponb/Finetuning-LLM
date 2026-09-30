from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(F.col("claim_type") == "theft")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("claim_type").isin("medical", "theft"), "group_a").when(F.col("claim_type") == "fire", "group_b").otherwise("other").alias("claim_type_group"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
)

claim_type_stats = (
    prepared_claims
    .groupBy("claim_type")
    .agg(F.max("approved_amount").alias("max_approved_amount"))
)

vs_group = (
    prepared_claims.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.approved_amount") == F.col("g.max_approved_amount"))
    .select(
        "s.*",
        "g.max_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.max_approved_amount"), 2).alias("approved_amount_minus_max"),
    )
)

enriched_policy = (
    vs_group.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.status_len",
        "s.claim_date_dayofweek",
        "s.max_approved_amount",
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    enriched_policy
    .select("id", "max_approved_amount", "claimed_amount")
    .unpivot(
        ids=["id"],
        values=["max_approved_amount", "claimed_amount"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
