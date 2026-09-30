from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("claim_status", "adjuster_id").orderBy("claimed_amount", "id", "claim_id")

valid_claims = (
    df_claims
    .filter((F.col("policy_id") >= 10) & (F.col("claim_type") == "flood"))
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
    )
)

detail_rows = (
    valid_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status") == "REJECTED"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

filtered = (
    detail_rows.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.hour(F.col("c.reported_at")).between(9, 13)), "left_anti")
)

unique_rows = (
    filtered
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_children = (
    unique_rows.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.reported_at",
        "s.claim_id",
        "s.claim_status",
        "s.claim_adjuster_id",
        "s.claim_claim_date",
        "s.claim_claimed_amount",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

with_details_claim = (
    with_children.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id_2")) & (F.year(F.col("c.claim_date")) == 2024), "inner")
    .filter(F.to_date(F.col("c.reported_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "s.*",
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.reported_at").alias("claim_reported_at_2"),
    )
)

result = (
    with_details_claim
    .groupBy("claim_policy_id", "policy_id")
    .pivot("claim_status", ["APPROVED", "IN_REVIEW", "PAID", "OPEN"])
    .agg(F.sum("claimed_amount"))
)
