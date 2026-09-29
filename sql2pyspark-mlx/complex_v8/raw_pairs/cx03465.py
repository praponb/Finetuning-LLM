from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter((F.col("claimed_amount") < 29359.4) & (F.year(F.col("claim_date")) == 2024))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.abs(F.col("claimed_amount") - 18778.6).alias("claimed_amount_distance"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

summary = (
    prepared
    .filter(F.col("policy_id") >= 21)
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "adjuster_id",
        "policy_id",
    )
    .agg(
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    summary
    .select(
        "claim_date_month",
        "adjuster_id",
        "cnt",
        "distinct_policy_ids",
    )
    .unpivot(
        ids=["claim_date_month", "adjuster_id"],
        values=["cnt", "distinct_policy_ids"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
