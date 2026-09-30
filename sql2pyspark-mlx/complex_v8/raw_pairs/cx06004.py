from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

with_children_claims = (
    filtered_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.status_prefix",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

without_matches = (
    with_children_claims
    .crossJoin(with_children_claims.agg(F.min("claimed_amount").alias("overall_min_claimed_amount")))
    .filter(F.col("claimed_amount") > F.col("overall_min_claimed_amount"))
    .drop("overall_min_claimed_amount")
)

enriched = (
    without_matches
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        "status_prefix",
        "claim_id",
        "claim_type",
        "claim_reported_at",
        "claim_claim_date",
        F.lower(F.regexp_replace(F.trim(F.col("status_prefix")), " ", "_")).alias("status_prefix_slug"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

result = (
    enriched
    .groupingSets(
        [["claim_id", "claim_type"], ["claim_id"], []],
        "claim_id", "claim_type",
    )
    .agg(F.count("*").alias("row_count"))
)
