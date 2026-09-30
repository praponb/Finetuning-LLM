from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("product").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
)

with_parent = (
    recent_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.end_date_month",
        "s.days_since_start_date",
        "p.holder_name",
    )
)

screened = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

per_group = (
    screened
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_date_ym"),
        "holder_id",
    )
    .agg(
        F.round(F.avg("days_since_start_date"), 2).alias("avg_days_since_start_date"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
    )
)

result = (
    per_group
    .select(
        "start_date_ym",
        "holder_id",
        "avg_days_since_start_date",
        "distinct_holder_ids",
        F.coalesce(F.col("distinct_holder_ids"), F.lit(0)).alias("distinct_holder_ids_filled"),
        F.coalesce(F.col("distinct_holder_ids"), F.lit(0)).alias("distinct_holder_ids_filled_2"),
    )
    .orderBy(F.col("avg_days_since_start_date").desc())
)
