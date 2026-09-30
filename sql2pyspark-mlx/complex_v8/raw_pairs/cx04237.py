from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.year(F.col("start_date")) == 2025)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

with_parent = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status_or_unknown",
        "s.product_slug",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

line_level = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") < 32796.4), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.product_slug",
        "s.holder_segment",
        "s.holder_joined_on",
        "s.holder_risk_score",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_type",
    )
)

joined_holder = (
    line_level.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.product_slug",
        "s.holder_joined_on",
        "s.holder_risk_score",
        "s.claim_id",
        "s.claim_claimed_amount",
        "s.claim_approved_amount",
        "s.claim_adjuster_id",
        "s.claim_type",
        F.col("p.risk_score").alias("holder_risk_score_2"),
    )
)

product_stats = (
    joined_holder
    .groupBy("product")
    .agg(F.avg("claim_approved_amount").alias("avg_claim_approved_amount"))
)

outliers = (
    joined_holder.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.claim_approved_amount") > F.col("g.avg_claim_approved_amount"))
    .select(
        "s.*",
        "g.avg_claim_approved_amount",
        F.round(F.col("s.claim_approved_amount") - F.col("g.avg_claim_approved_amount"), 2).alias("claim_approved_amount_minus_avg"),
    )
)

result = (
    outliers
    .filter(F.col("claim_claimed_amount") < 29336.0)
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "product_slug",
        "holder_risk_score",
        "claim_claimed_amount",
        "claim_approved_amount",
        "claim_adjuster_id",
        "claim_type",
        "holder_risk_score_2",
        "avg_claim_approved_amount",
        F.least(F.col("holder_risk_score"), F.col("claim_approved_amount")).alias("least_holder_risk_score_claim_approved_amount"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
