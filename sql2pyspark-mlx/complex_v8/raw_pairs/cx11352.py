from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("approved_amount") >= 5336.7) | (F.col("claim_type").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd HH:00").alias("reported_at_label"),
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
    )
)

with_details_policy = (
    filtered_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select("s.*", F.col("p.premium").alias("policy_premium"))
)

per_group = (
    with_details_policy
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "policy_id",
        F.date_trunc("month", "claim_date_month_end").alias("claim_date_month_end_month"),
    )
    .agg(
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("policy_premium"), 2).alias("stddev_policy_premium"),
    )
)

screened = (
    per_group
    .crossJoin(per_group.agg(F.avg("total_approved_amount").alias("overall_avg_total_approved_amount")))
    .filter(F.col("total_approved_amount") > F.col("overall_avg_total_approved_amount"))
    .drop("overall_avg_total_approved_amount")
)

result = (
    screened
    .select(
        "claim_date_month",
        "policy_id",
        "claim_date_month_end_month",
        "total_approved_amount",
        "stddev_policy_premium",
        (F.col("claim_date_month") + F.expr("INTERVAL 2 HOURS")).alias("claim_date_month_plus_2h"),
        F.round(F.col("total_approved_amount") / F.nullif(F.col("row_count"), F.lit(0)), 3).alias("total_approved_amount_per_row_count"),
    )
    .orderBy(
        F.col("total_approved_amount").desc(),
        "claim_date_month",
        "policy_id",
        "claim_date_month_end_month",
    )
    .limit(5)
)
