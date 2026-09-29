from pyspark.sql import functions as F

recent = (
    df_claims
    .filter(F.col("status").isin("OPEN", "IN_REVIEW", "APPROVED"))
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

claim_rows = (
    recent
    .filter((F.month(F.col("reported_at")).isin(1, 3)) & (F.col("claimed_amount") <= 34401.0))
    .select(
        F.lit("claim").alias("record_type"),
        "adjuster_id",
        "claimed_amount",
        F.col("claim_date").alias("event_date"),
    )
)

policy_rows = (
    df_policies
    .select(
        F.lit("policy").alias("record_type"),
        F.col("holder_id").alias("adjuster_id"),
        F.col("premium").alias("claimed_amount"),
        F.col("end_date").alias("event_date"),
    )
)

holder_side = (
    df_policyholders
    .select(
        F.lit("holder").alias("record_type"),
        F.col("id").alias("adjuster_id"),
        F.col("risk_score").alias("claimed_amount"),
        F.col("joined_on").alias("event_date"),
    )
)

result = (
    claim_rows
    .unionByName(policy_rows)
    .unionByName(holder_side)
    .orderBy("claimed_amount", "adjuster_id")
)
