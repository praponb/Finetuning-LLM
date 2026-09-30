from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        F.concat_ws("-", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

filtered = (
    recent_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") >= 13635.1), "left_anti")
)

result = (
    filtered
    .select("policy_id")
    .distinct()
    .orderBy(F.col("policy_id").desc())
)
