from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(8, 12))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
)

filtered = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium") >= 2096.6) & (F.col("p.product") == "TRAVEL"), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "claimed_amount_minus_approved_amount",
        "least_approved_amount_claimed_amount",
        "claimed_amount",
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount_minus_approved_amount", "least_approved_amount_claimed_amount", "claimed_amount"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
