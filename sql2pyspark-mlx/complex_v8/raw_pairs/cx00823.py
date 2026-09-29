from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(F.col("claimed_amount") > 18364.1)
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
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.date_trunc("year", F.col("reported_at")).alias("reported_at_year"),
    )
)

exploded_detail_claims = (
    prepared_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claim_type_len",
        "s.reported_at_year",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.status").alias("claim_status"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

with_details_claim = (
    exploded_detail_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.*",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.reported_at").alias("claim_reported_at_2"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
    )
)

with_counts = (
    with_details_claim.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.avg_claim_claimed_amount",
        "c.total_claim_approved_amount",
    )
)

result = (
    with_counts
    .filter(F.col("claim_type") != "flood")
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at_year",
        "claim_id",
        "claim_claim_type",
        "claim_status",
        "claim_reported_at",
        "claim_approved_amount",
        "total_claim_approved_amount",
    )
    .orderBy("claim_reported_at", "id", "claim_id")
    .limit(5)
)
