from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("policy_id") <= 20) | (F.col("status") == "APPROVED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

enriched = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.upper(F.trim(F.col("a.adjuster_name"))).like("G%")), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .filter(F.col("a.level") <= 3)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.claimed_amount_minus_approved_amount",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    enriched
    .cube("policy_id", "adjuster_id")
    .agg(
        F.grouping("policy_id").alias("is_total_policy_id"),
        F.max("adjuster_level").alias("max_adjuster_level"),
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
