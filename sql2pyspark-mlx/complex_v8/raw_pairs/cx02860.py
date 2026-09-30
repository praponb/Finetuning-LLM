from pyspark.sql import functions as F

valid_claims = (
    df_claims
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

stats = (
    valid_claims
    .groupBy("adjuster_id", F.year("reported_at").alias("reported_at_year"))
    .agg(
        F.max("approved_amount").alias("max_approved_amount"),
        F.count("*").alias("n_rows"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.min("claimed_amount").alias("min_claimed_amount"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("n_rows") >= 2)
)

with_activity = (
    stats
    .crossJoin(stats.agg(F.min("n_rows").alias("overall_min_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_min_n_rows"))
    .drop("overall_min_n_rows")
)

with_children = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.adjuster_id",
        "s.reported_at_year",
        "s.max_approved_amount",
        "s.distinct_ids",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

qualified = (
    with_children.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.claim_policy_id")) & (F.col("c.claimed_amount") > 5774.6), "left_anti")
    .filter(F.col("claim_approved_amount") >= 0.0)
)

result = (
    qualified
    .groupingSets(
        [["adjuster_id", "claim_id", "distinct_ids"], ["adjuster_id"]],
        "adjuster_id", "claim_id", "distinct_ids",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("claim_approved_amount").alias("max_claim_approved_amount"),
        F.round(F.avg("claim_approved_amount"), 2).alias("avg_claim_approved_amount"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
        F.col("distinct_ids").asc_nulls_last(),
    )
)
