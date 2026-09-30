from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(2, 3)) | (F.col("claimed_amount") <= 31752.9))
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
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

tagged = recent_claims.select("*", F.lower(F.trim(F.col("status"))).alias("status_clean"))

combined_details_adjuster = (
    tagged.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team") == "Health"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .filter(F.length(F.trim(F.col("a.adjuster_name"))) >= 4)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.approved_amount_filled",
        "s.status_len",
        "s.status_clean",
        "a.adjuster_name",
        F.col("p.product").alias("policy_product"),
    )
)

qualified = (
    combined_details_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.reported_at") < F.lit("2025-03-02 14:00:00").cast("timestamp")) & (F.col("c.claimed_amount") >= 18924.4), "left_anti")
)

result = (
    qualified
    .groupBy("policy_id", "claim_date")
    .pivot("claim_type", ["fire", "medical", "theft"])
    .agg(F.max("approved_amount_filled"))
)
