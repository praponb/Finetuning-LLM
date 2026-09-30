from pyspark.sql import functions as F

recent = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(10, 12))
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
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

tagged = (
    recent
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status",
        "reported_at",
        "claim_type_clean",
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
    )
)

result = (
    tagged
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "reported_at",
        "claim_type_clean",
        "reported_at_plus_2h",
    )
    .orderBy(
        F.col("reported_at_plus_2h").asc_nulls_last(),
        F.col("claim_type_clean").desc(),
    )
)
