from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter(F.col("status").isin("OPEN", "APPROVED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

without_matches = (
    filtered_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status") == "PAID") & (F.col("c.approved_amount") <= 18478.3), "left_semi")
)

screened = (
    without_matches.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.hour(F.col("c.reported_at")).between(8, 14)), "left_semi")
)

with_activity = (
    screened.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id") >= 21), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "status_prefix",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix_2"),
    )
    .orderBy(F.col("status").desc(), F.col("id").desc())
)
