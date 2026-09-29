from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter(~F.col("claim_type").isin("flood", "medical"))
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
        F.when(F.col("status").isin("APPROVED", "IN_REVIEW"), "group_a").when(F.col("status") == "REJECTED", "group_b").otherwise("other").alias("status_group"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

per_group = (
    prepared
    .filter(F.col("policy_id").isNotNull())
    .groupBy("adjuster_id", "status_group")
    .agg(
        F.round(100 * F.count(F.when(F.col("status") == "IN_REVIEW", 1)) / F.count("*"), 1).alias("pct_status_in_review"),
        F.round(F.sum("approved_amount") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("approved_amount_to_claimed_amount_ratio"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
        F.min("approved_amount").alias("min_approved_amount"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
    )
)

joined = (
    per_group.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.lower(F.col("a.adjuster_name")).like("% s%")), "inner")
    .select(
        "s.adjuster_id",
        "s.status_group",
        "s.pct_status_in_review",
        "s.distinct_claim_types",
        "s.min_approved_amount",
        "s.distinct_adjuster_ids",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

result = (
    joined
    .groupBy("adjuster_level")
    .pivot("status_group", ["group_a", "group_b", "other"])
    .agg(F.sum("min_approved_amount"))
)
