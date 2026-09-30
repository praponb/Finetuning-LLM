from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter(F.col("adjuster_id") >= 2)
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
        F.abs(F.col("approved_amount") - 18478.3).alias("approved_amount_distance"),
    )
)

with_activity = (
    scoped_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date") >= F.lit("2024-06-27").cast("date")) & (F.col("c.policy_id") <= 28), "left_semi")
)

per_group = (
    with_activity
    .groupBy("policy_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("claim_type") == "delay", 1)) / F.count("*"), 1).alias("pct_claim_type_delay"),
        F.sum(F.when(F.col("status").isNotNull(), F.col("approved_amount_distance")).otherwise(0)).alias("approved_amount_distance_status_is_not_null"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.sum("approved_amount").alias("total_approved_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
    .filter(F.col("total_approved_amount") > 0.0)
)

qualified = (
    per_group.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.reported_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")), "left_semi")
)

result = (
    qualified
    .select(
        "policy_id",
        "approved_amount_distance_status_is_not_null",
        "total_approved_amount",
        "pct_claim_type_delay",
    )
    .unpivot(
        ids=["policy_id"],
        values=["approved_amount_distance_status_is_not_null", "total_approved_amount", "pct_claim_type_delay"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
