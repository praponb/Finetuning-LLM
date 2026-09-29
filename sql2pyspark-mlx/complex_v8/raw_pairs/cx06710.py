from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("policy_id") <= 29) & (F.col("claim_type") == "theft"))
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
        F.abs(F.col("claimed_amount") - 17759.3).alias("claimed_amount_distance"),
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
    )
)

without_matches = (
    valid_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status") != "REJECTED") & (F.col("c.claim_type") == "flood"), "left_semi")
)

detail_rows = (
    without_matches.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id") <= 4), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_distance",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "claim_date",
        "status",
        "reported_at",
        "claimed_amount_distance",
        "claim_claim_type",
    )
    .orderBy(F.col("reported_at").desc_nulls_first(), F.col("id").asc_nulls_last())
)
