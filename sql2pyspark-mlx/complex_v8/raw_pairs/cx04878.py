from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.col("adjuster_id") >= 3) & (F.col("claimed_amount") > 15381.6))
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
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.hour(F.col("reported_at")).alias("reported_at_hour"),
    )
)

filtered = (
    cleaned_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

qualified = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isNotNull()), "left_semi")
)

enriched = (
    qualified.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select("s.*", F.col("p.premium").alias("policy_premium"))
)

result = (
    enriched
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
        "status_prefix",
        "reported_at_hour",
        "policy_premium",
        F.abs(F.col("policy_premium") - 3521.8).alias("policy_premium_distance"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
    .orderBy(F.col("approved_amount").desc(), "id")
    .limit(5)
)
