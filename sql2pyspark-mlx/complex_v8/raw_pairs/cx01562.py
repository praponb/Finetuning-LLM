from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-02-24 07:00:00").cast("timestamp")) & (F.col("approved_amount") <= 20843.1))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.quarter(F.col("reported_at")).alias("reported_at_quarter"),
    )
)

qualified = (
    base_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") >= 2887.3), "left_anti")
)

with_details = (
    qualified.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.reported_at",
        "s.reported_at_quarter",
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    with_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
        "reported_at_quarter",
        "policy_status",
    )
    .orderBy(F.col("claim_type").desc())
)
