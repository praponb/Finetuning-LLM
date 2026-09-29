from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id").orderBy("claimed_amount_distance", "id")

prepared = (
    df_claims
    .filter(F.col("approved_amount") <= 28426.9)
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
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.abs(F.col("claimed_amount") - 20436.5).alias("claimed_amount_distance"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

screened = (
    prepared.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
    .filter(F.col("claimed_amount").between(10518.4, 33975.8))
)

deduped = (
    screened
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined_policy = (
    deduped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claimed_amount_distance",
        "s.approved_amount_filled",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.premium").alias("policy_premium"),
    )
)

enriched = (
    joined_policy
    .filter(F.col("claim_date") < F.lit("2025-09-19").cast("date"))
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "approved_amount_filled",
        "policy_holder_id",
        F.least(F.col("approved_amount_filled"), F.col("claimed_amount")).alias("least_approved_amount_filled_claimed_amount"),
    )
)

result = (
    enriched
    .groupBy("policy_holder_id", "claim_type")
    .pivot("status", ["OPEN", "REJECTED", "IN_REVIEW"])
    .agg(F.sum("approved_amount_filled"))
    .orderBy("policy_holder_id", "claim_type")
)
