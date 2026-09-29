from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("approved_amount") >= 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
        F.concat_ws(" / ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

with_activity = (
    valid
    .crossJoin(valid.agg(F.min("claimed_amount").alias("overall_min_claimed_amount")))
    .filter(F.col("claimed_amount") > F.col("overall_min_claimed_amount"))
    .drop("overall_min_claimed_amount")
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
    )
)

with_counts = (
    with_activity.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.min_claim_approved_amount",
        "c.total_claim_approved_amount",
    )
)

scored = (
    with_counts
    .filter(F.col("reported_at_plus_2h") >= F.lit("2025-01-09 09:00:00").cast("timestamp"))
    .select(
        "id",
        "claimed_amount",
        "status",
        "reported_at",
        "claim_type_clean",
        "reported_at_plus_2h",
        "claim_type_status_label",
        "claim_count",
        "min_claim_approved_amount",
        "total_claim_approved_amount",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

result = (
    scored
    .select("id", "min_claim_approved_amount", "total_claim_approved_amount")
    .unpivot(
        ids=["id"],
        values=["min_claim_approved_amount", "total_claim_approved_amount"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric_name")
)
