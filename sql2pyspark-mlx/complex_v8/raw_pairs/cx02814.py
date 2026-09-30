from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(1, 3)) & (F.col("approved_amount") >= 0.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_children = (
    scoped.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
    )
)

annotated = (
    with_children
    .select(
        "*",
        F.when(F.col("approved_amount") >= 18478.3, "A").when(F.col("approved_amount") >= 7204.1, "B").otherwise("C").alias("approved_amount_tier"),
        F.date_format(F.col("claim_reported_at"), "yyyy-MM-dd HH:00").alias("claim_reported_at_label"),
    )
)

result = (
    annotated
    .filter(F.col("claim_id") >= 59)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        "claim_id",
        "claim_reported_at",
        "approved_amount_tier",
        "claim_reported_at_label",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
    .orderBy("status_or_unknown", F.col("reported_at").desc())
)
