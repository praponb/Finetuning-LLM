from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter((F.col("claim_date").isNotNull()) & (~F.col("status").isin("OPEN", "PAID")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.abs(F.col("approved_amount") - 15857.9).alias("approved_amount_distance"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

joined = (
    base_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.approved_amount_distance",
        "s.claimed_amount_filled",
        "s.claim_type_clean",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.max("claim_date").alias("last_claim_date"),
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("claim_status_count"),
    )
)

with_counts = (
    joined.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.avg_claim_approved_amount",
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "claimed_amount_filled",
        "adjuster_name",
        "adjuster_level",
        "avg_claim_approved_amount",
        "last_claim_date",
        "claim_status_count",
        F.length(F.trim(F.col("claim_type_clean"))).alias("claim_type_clean_len"),
        (F.col("claimed_amount") - F.col("approved_amount_distance")).alias("claimed_amount_minus_approved_amount_distance"),
    )
    .orderBy(F.col("adjuster_id").asc_nulls_last(), "id")
    .limit(20)
)
