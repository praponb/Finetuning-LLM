from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("claim_type") == "flood"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        (F.col("reported_at") + F.expr("INTERVAL 1 DAYS")).alias("reported_at_plus_1d"),
    )
)

exploded_detail_claims = (
    filtered.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

result = (
    exploded_detail_claims
    .rollup("adjuster_id", "claim_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.max("claim_claim_date").alias("last_claim_claim_date"),
    )
)
