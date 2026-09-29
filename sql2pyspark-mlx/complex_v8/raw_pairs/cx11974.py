from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("policy_id") >= 9) & (F.col("claim_date").between(F.lit("2024-03-29").cast("date"), F.lit("2025-11-15").cast("date"))))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

qualified = (
    base.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status") != "IN_REVIEW"), "left_semi")
)

joined_policy = (
    qualified.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.reported_at",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    joined_policy
    .filter(F.col("policy_id") <= 16)
    .select(
        "id",
        "adjuster_id",
        "reported_at",
        "policy_end_date",
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(10)
)
