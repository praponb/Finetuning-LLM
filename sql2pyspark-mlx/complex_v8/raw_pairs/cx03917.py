from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-03-02 11:00:00").cast("timestamp"))
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
        F.when(F.col("claimed_amount") >= 41760.8, "large").when(F.col("claimed_amount") >= 11235.6, "medium").otherwise("small").alias("claimed_amount_tier"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(~F.col("claim_type").isin("medical", "collision"), 1)).alias("claim_claim_type_count"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

with_stats = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.max_claim_approved_amount",
        "c.first_claim_date",
    )
)

without_matches = (
    with_stats.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (~F.col("p.product").isin("HEALTH", "LIFE")) & (~F.col("p.status").isin("ACTIVE", "CANCELLED")), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        F.col("approved_amount").alias("approved_amount_v"),
        F.col("claimed_amount").alias("claimed_amount_v"),
    )
    .unpivot(
        ids=["id"],
        values=["approved_amount_v", "claimed_amount_v"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
