from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter(F.col("policy_id") >= 28)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

with_children_claims = (
    scoped_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status_ref",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
        "c.claim_date",
    )
)

scored = (
    with_children_claims
    .select("*", F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"))
)

result = (
    scored
    .cube("claim_id", "claim_claim_type", "claim_type")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("claim_approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.round(F.avg("claim_date_dayofweek"), 2).alias("avg_claim_date_dayofweek"),
    )
    .select(
        "claim_id",
        F.coalesce(F.col("claim_claim_type"), F.lit("ALL")).alias("claim_claim_type"),
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "cnt",
        "avg_claim_approved_amount",
        "avg_claim_date_dayofweek",
    )
)
