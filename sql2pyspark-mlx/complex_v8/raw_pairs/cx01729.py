from pyspark.sql import functions as F

cleaned = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("claimed_amount") >= 40638.4, "large").when(F.col("claimed_amount") >= 19347.2, "medium").otherwise("small").alias("claimed_amount_tier"),
    )
)

derived = (
    cleaned
    .filter(F.col("policy_id") >= 5)
    .select(
        "*",
        F.hour(F.col("reported_at")).alias("reported_at_hour"),
        F.when(F.col("approved_amount") >= 19337.6, "HIGH").when(F.col("approved_amount") >= 799.5, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
    )
)

with_details = (
    derived.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

qualified = (
    with_details.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team") == "Motor") & (F.col("a.level") <= 3), "left_semi")
)

with_parent = (
    qualified.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium") > 2289.0), "inner")
    .filter(F.col("p.product").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.approved_amount_tier",
        "s.adjuster_name",
        "s.adjuster_team",
        "s.adjuster_level",
        F.col("p.product").alias("policy_product"),
    )
)

detail_rows_claims = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id") >= 16), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "s.adjuster_team",
        "s.adjuster_level",
        "s.policy_product",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    detail_rows_claims
    .select(
        "id",
        "policy_id",
        "claim_type",
        "status",
        "policy_product",
        "claim_id",
        "claim_policy_id",
        F.abs(F.col("claim_approved_amount") - 17759.3).alias("claim_approved_amount_distance"),
        F.coalesce(F.col("policy_product"), F.lit("UNKNOWN")).alias("policy_product_or_unknown"),
    )
    .orderBy(
        F.col("claim_approved_amount_distance").asc_nulls_last(),
        "id",
        "claim_id",
    )
    .limit(20)
)
