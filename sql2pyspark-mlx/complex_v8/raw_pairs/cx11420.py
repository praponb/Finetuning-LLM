from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.year(F.col("claim_date")) == 2025) & (F.col("status") == "REJECTED"))
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
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.status_ref",
        F.col("a.level").alias("adjuster_level"),
    )
)

with_parent_policy = (
    with_details.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.status_ref",
        "s.adjuster_level",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("reported_at").alias("last_claim_reported_at"),
        F.count(F.when(F.col("status").isin("OPEN", "APPROVED"), 1)).alias("claim_status_count"),
    )
)

summarized_rows = (
    with_parent_policy.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_reported_at",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

exploded_detail_claims = (
    summarized_rows.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_type").isin("delay", "theft")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status_ref",
        "s.policy_premium",
        "s.claim_status_count",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    exploded_detail_claims
    .groupBy("claim_claim_type", "claim_id")
    .pivot("claim_type", ["fire", "collision", "theft"])
    .agg(F.avg("policy_premium"))
)
