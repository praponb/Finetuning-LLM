from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-04-20").cast("date")) & (F.col("status").isin("REJECTED", "PAID")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

exploded_detail = (
    valid_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
    )
)

with_counts = (
    exploded_detail.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.avg_claim_approved_amount")
)

enriched_policy = (
    with_counts.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.premium") >= 1541.8)
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
    )
)

with_activity = (
    enriched_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") > 7204.1) & (F.col("c.status").isin("OPEN", "PAID", "IN_REVIEW")), "left_anti")
    .filter(F.col("policy_product") != "TRAVEL")
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

with_counts_2 = (
    with_activity.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

annotated = (
    with_counts_2
    .filter(F.col("claim_id") >= 44)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "claim_id",
        "claim_reported_at",
        "avg_claim_approved_amount",
        "policy_premium",
        "claim_count",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("policy_end_date")).alias("days_since_policy_end_date"),
        F.abs(F.col("policy_premium") - 2721.8).alias("policy_premium_distance"),
    )
)

result = (
    annotated
    .groupBy("adjuster_id", "claim_reported_at")
    .pivot("claim_type", ["theft", "medical"])
    .agg(F.max("claim_count"))
    .orderBy("adjuster_id", "claim_reported_at")
)
