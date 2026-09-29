from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter(F.col("approved_amount") > 2887.3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_parent = (
    filtered_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.length(F.trim(F.col("a.adjuster_name"))) >= 5), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

grouped = (
    with_parent
    .groupBy("policy_id", "adjuster_id")
    .agg(
        F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"),
        F.sum(F.when(F.col("adjuster_team").isNotNull(), F.col("adjuster_level"))).alias("adjuster_level_adjuster_team_is_not_null"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
    )
)

annotated = (
    grouped
    .filter(F.col("policy_id") >= 25)
    .select(
        "*",
        F.coalesce(F.col("median_approved_amount"), F.lit(0.0)).alias("median_approved_amount_filled"),
        (F.floor(F.col("median_approved_amount") / 10000) * 10000).alias("median_approved_amount_bucket"),
        (F.col("distinct_claim_types") - F.col("median_approved_amount")).alias("distinct_claim_types_minus_median_approved_amoun"),
    )
)

result = (
    annotated
    .select(
        "policy_id",
        "adjuster_id",
        F.col("median_approved_amount_bucket").alias("median_approved_amount_bucket_v"),
        F.col("adjuster_level_adjuster_team_is_not_null").alias("m_adjuster_level_adjuster_team_is_not_null"),
    )
    .unpivot(
        ids=["policy_id", "adjuster_id"],
        values=["median_approved_amount_bucket_v", "m_adjuster_level_adjuster_team_is_not_null"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .orderBy("policy_id", "adjuster_id", "metric")
)
