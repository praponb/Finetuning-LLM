from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
    )
)

adjuster_id_stats = (
    prepared_claims
    .groupBy("adjuster_id")
    .agg(F.max("approved_amount").alias("max_approved_amount"))
)

above_average = (
    prepared_claims.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.approved_amount") == F.col("g.max_approved_amount"))
    .select(
        "s.*",
        "g.max_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.max_approved_amount"), 2).alias("approved_amount_minus_max"),
    )
)

enriched = (
    above_average.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.approved_amount_minus_max",
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    enriched
    .select(
        "id",
        "policy_id",
        "claim_type",
        "status",
        "approved_amount_minus_max",
    )
    .orderBy(F.col("approved_amount_minus_max").desc())
)
