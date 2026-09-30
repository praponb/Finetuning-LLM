from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
        F.round(F.col("claimed_amount") * 0.9, 2).alias("claimed_amount_adj"),
    )
)

with_children = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.claim_date_year",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        "c.claim_date",
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

joined = (
    with_children.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.claim_policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.claim_date_year",
        "s.claim_id",
        "s.claim_policy_id",
        "s.claim_date",
        "s.claim_approved_amount",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    joined
    .groupingSets(
        [["claim_id", "status", "policy_id"], ["claim_id"]],
        "claim_id", "status", "policy_id",
    )
    .agg(F.count("*").alias("n_rows"))
    .orderBy(
        F.col("claim_id").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
