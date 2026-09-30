from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("adjuster_id") <= 5)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.abs(F.col("claimed_amount") - 14019.8).alias("claimed_amount_distance"),
        F.quarter(F.col("reported_at")).alias("reported_at_quarter"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.end_date") >= F.lit("2026-01-20").cast("date")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.reported_at",
        "s.claimed_amount_distance",
        "s.reported_at_quarter",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    with_details
    .select(
        "id",
        F.col("claimed_amount_distance").alias("claimed_amount_distance_v"),
        F.col("policy_premium").alias("m_policy_premium"),
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount_distance_v", "m_policy_premium"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
