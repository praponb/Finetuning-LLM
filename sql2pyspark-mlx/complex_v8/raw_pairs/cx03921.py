from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        F.when(F.col("status").isin("REJECTED", "IN_REVIEW"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other").alias("status_group"),
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
)

with_parent = (
    scoped_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status",
        "s.reported_at",
        "s.status_group",
        "s.claimed_amount_minus_approved_amount",
        F.col("p.status").alias("policy_status"),
    )
)

screened = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount").between(0.0, 20880.4)), "left_semi")
)

result = (
    screened
    .groupingSets(
        [["policy_id", "status", "adjuster_id"], ["policy_id"], []],
        "policy_id", "status", "adjuster_id",
    )
    .agg(
        F.round(F.avg("claimed_amount_minus_approved_amount"), 2).alias("avg_claimed_amount_minus_approved_amount"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
