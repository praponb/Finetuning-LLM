from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("claimed_amount") > 12010.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count(F.when(F.col("status").isin("APPROVED", "IN_REVIEW", "PAID"), 1)).alias("claim_status_count"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
    )
)

with_stats = (
    prepared.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.min_claim_claimed_amount",
    )
)

result = (
    with_stats
    .select("id", "claimed_amount_bucket", "claim_status_count")
    .unpivot(
        ids=["id"],
        values=["claimed_amount_bucket", "claim_status_count"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
