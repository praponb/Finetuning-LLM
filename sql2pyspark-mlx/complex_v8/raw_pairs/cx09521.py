from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(6, 8))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd HH:00").alias("reported_at_label"),
    )
)

policy_id_stats = (
    valid_claims
    .groupBy("policy_id")
    .agg(F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"))
)

vs_group = (
    valid_claims.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.approved_amount") >= F.col("g.median_approved_amount"))
    .select(
        "s.*",
        "g.median_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.median_approved_amount"), 2).alias("approved_amount_minus_median"),
    )
)

qualified = (
    vs_group.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level").between(2, 3)), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        F.col("approved_amount").alias("approved_amount_v"),
        F.col("median_approved_amount").alias("m_median_approved_amount"),
        F.col("approved_amount_minus_median").alias("approved_amount_minus_median_v"),
    )
    .unpivot(
        ids=["id"],
        values=["approved_amount_v", "m_median_approved_amount", "approved_amount_minus_median_v"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
