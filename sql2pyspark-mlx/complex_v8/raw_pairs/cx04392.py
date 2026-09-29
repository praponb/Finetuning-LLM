from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter((F.col("status").isin("PAID", "REJECTED")) & (F.col("reported_at") < F.lit("2025-03-02 12:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
    )
)

without_matches = (
    recent_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.month(F.col("c.reported_at")).isin(1, 3)), "left_anti")
)

result = (
    without_matches
    .rollup("adjuster_id", "policy_id")
    .agg(
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.round(F.avg("approved_amount"), 2).alias("avg_approved_amount"),
        F.min("approved_amount").alias("min_approved_amount"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
