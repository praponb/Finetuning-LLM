from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.col("holder_id") >= 2)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
    )
)

enriched_holder = (
    scoped_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.risk_score") <= 0.6)
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

without_matches = (
    enriched_holder.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") > 2289.0) & (F.col("p.start_date") < F.lit("2025-05-02").cast("date")), "left_semi")
)

line_level_claims = (
    without_matches.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.holder_id",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    line_level_claims
    .groupingSets(
        [["holder_id", "claim_id"], ["holder_id"]],
        "holder_id", "claim_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("claim_reported_at").alias("last_claim_reported_at"),
        F.percentile_approx("claim_claimed_amount", 0.5).alias("median_claim_claimed_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
)
