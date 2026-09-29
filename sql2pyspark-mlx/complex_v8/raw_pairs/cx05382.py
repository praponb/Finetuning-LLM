from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
    )
)

rollup_base = (
    recent_claims
    .filter(F.col("approved_amount") <= 28426.9)
    .groupBy("adjuster_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_approved_amount"),
        F.count("*").alias("row_count"),
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"),
        F.max("reported_at").alias("last_reported_at"),
        F.count(F.when(~F.col("claim_type").isin("delay", "collision"), 1)).alias("n_claim_type_not_in_delay_coll"),
    )
    .filter(F.col("row_count") >= 2)
)

annotated = (
    rollup_base
    .select(
        "*",
        F.abs(F.col("total_approved_amount") - 105000.0).alias("total_approved_amount_distance"),
        F.abs(F.col("total_approved_amount") - 120000.0).alias("total_approved_amount_distance_2"),
    )
)

result = (
    annotated
    .select(
        "adjuster_id",
        "total_approved_amount",
        "total_approved_amount_distance",
        "total_approved_amount_distance_2",
        "avg_approved_amount",
    )
    .unpivot(
        ids=["adjuster_id"],
        values=["total_approved_amount", "total_approved_amount_distance", "total_approved_amount_distance_2", "avg_approved_amount"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("adjuster_id", "metric")
)
