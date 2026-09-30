from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

exploded_detail_claims = (
    valid_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.greatest_claimed_amount_approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
    )
)

grouped = (
    exploded_detail_claims
    .filter(F.col("greatest_claimed_amount_approved_amount") < 42500.0)
    .groupBy("adjuster_id", "claim_status")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("greatest_claimed_amount_approved_amount"), 2).alias("avg_greatest_claimed_amount_approved_amount"),
        F.max("greatest_claimed_amount_approved_amount").alias("max_greatest_claimed_amount_approved_amount"),
    )
)

combined_details = (
    grouped.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.adjuster_id",
        "s.claim_status",
        "s.row_count",
        "s.distinct_ids",
        "s.avg_greatest_claimed_amount_approved_amount",
        "s.max_greatest_claimed_amount_approved_amount",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    combined_details
    .select(
        "adjuster_id",
        "claim_status",
        "avg_greatest_claimed_amount_approved_amount",
        "max_greatest_claimed_amount_approved_amount",
    )
    .unpivot(
        ids=["adjuster_id", "claim_status"],
        values=["avg_greatest_claimed_amount_approved_amount", "max_greatest_claimed_amount_approved_amount"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
