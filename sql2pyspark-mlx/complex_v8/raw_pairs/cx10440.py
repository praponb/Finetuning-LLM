from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.col("policy_id") <= 29) & (F.col("claim_type").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

detail_rows = (
    cleaned_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.claim_date_quarter",
        "s.reported_at_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

line_level_claims = (
    detail_rows.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date_quarter",
        "s.reported_at_date",
        "s.claim_id",
        "s.claim_approved_amount",
        "s.claim_status",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.claimed_amount").alias("claim_claimed_amount_2"),
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
    )
)

result = (
    line_level_claims
    .groupBy("claim_id_2")
    .pivot("claim_status", ["IN_REVIEW", "OPEN", "REJECTED", "APPROVED"])
    .agg(F.sum("claim_approved_amount_2").alias("amount"), F.count("claim_approved_amount_2").alias("n"))
    .orderBy("claim_id_2")
)
