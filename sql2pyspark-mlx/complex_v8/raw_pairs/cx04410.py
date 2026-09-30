from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter(F.col("claimed_amount") < 32441.2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat_ws("-", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
    )
)

adjuster_id_stats = (
    prepared
    .groupBy("adjuster_id")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

outliers = (
    prepared.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        F.col("claimed_amount").alias("claimed_amount_v"),
        F.col("approved_amount_minus_avg").alias("approved_amount_minus_avg_v"),
        F.col("approved_amount").alias("m_approved_amount"),
        F.col("avg_approved_amount").alias("m_avg_approved_amount"),
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount_v", "approved_amount_minus_avg_v", "m_approved_amount", "m_avg_approved_amount"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
