from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(F.col("claim_type") == "flood")
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
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

with_stats = (
    prepared_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.first_claim_date",
        "c.total_claim_approved_amount",
        "c.first_claim_reported_at",
    )
)

with_activity = (
    with_stats
    .crossJoin(with_stats.agg(F.avg("total_claim_approved_amount").alias("overall_avg_total_claim_approved_amount")))
    .filter(F.col("total_claim_approved_amount") > F.col("overall_avg_total_claim_approved_amount"))
    .drop("overall_avg_total_claim_approved_amount")
)

without_matches = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id") <= 5), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "total_claim_approved_amount",
        "first_claim_reported_at",
    )
    .orderBy(F.col("first_claim_reported_at").asc(), "claimed_amount")
)
