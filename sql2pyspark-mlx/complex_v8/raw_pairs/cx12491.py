from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("claim_type").isin("collision", "theft"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.when(F.col("status").isin("OPEN", "APPROVED"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
    )
)

detail_rows = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (~F.col("c.claim_type").isin("delay", "medical")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.status_group",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.max("claim_date").alias("last_claim_date_2"),
        F.count(F.when(F.col("claim_type") == "collision", 1)).alias("claim_claim_type_count"),
    )
)

summarized_rows = (
    detail_rows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.last_claim_date",
        "c.last_claim_date_2",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    summarized_rows
    .groupBy("claim_id")
    .pivot("claim_type", ["collision", "fire"])
    .agg(F.avg("claimed_amount"))
)
