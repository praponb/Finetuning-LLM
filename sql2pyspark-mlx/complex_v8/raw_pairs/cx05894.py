from pyspark.sql import functions as F

base_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
    )
)

with_children = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.start_date_label",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

rollup_base = (
    with_children
    .groupBy(
        F.date_trunc("month", "start_date").alias("start_date_month"),
        F.date_format("claim_reported_at", "yyyy-MM").alias("claim_reported_at_ym"),
    )
    .agg(
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("n_status_active"),
        F.count(F.when(F.col("status") != "LAPSED", 1)).alias("n_status_lapsed"),
        F.countDistinct("claim_id").alias("distinct_claim_ids"),
    )
)

annotated = (
    rollup_base
    .select(
        "*",
        (F.floor(F.col("n_status_active") / 2) * 2).alias("n_status_active_bucket"),
        F.dayofweek(F.col("start_date_month")).alias("start_date_month_dayofweek"),
        F.abs(F.col("n_status_lapsed") - 5).alias("n_status_lapsed_distance"),
    )
)

result = (
    annotated
    .select(
        "start_date_month",
        "claim_reported_at_ym",
        "n_status_lapsed",
        "n_status_active_bucket",
        "start_date_month_dayofweek",
        "n_status_lapsed_distance",
    )
    .orderBy(
        F.col("n_status_lapsed_distance").asc_nulls_last(),
        F.col("start_date_month").desc(),
    )
)
