from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("policy_id") <= 25) & (F.col("adjuster_id") <= 2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

with_counts = (
    filtered_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.max_claim_claimed_amount", "c.last_claim_date")
)

filtered = (
    with_counts.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") < 38909.4), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        F.col("max_claim_claimed_amount").alias("m_max_claim_claimed_amount"),
        F.col("claimed_amount").alias("claimed_amount_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_max_claim_claimed_amount", "claimed_amount_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
