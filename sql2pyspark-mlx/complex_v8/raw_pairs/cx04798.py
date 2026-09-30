from pyspark.sql import functions as F

cleaned_holders = df_policyholders.filter(F.col("joined_on") >= F.lit("2024-06-29").cast("date"))

with_activity = (
    cleaned_holders.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.premium") <= 4670.9) & (F.col("p.start_date") < F.lit("2025-05-02").cast("date")), "left_semi")
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("end_date").alias("last_policy_end_date"),
        F.count("*").alias("policy_count"),
    )
)

with_stats = (
    with_activity.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_policy_end_date",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

summary = (
    with_stats
    .groupBy("region")
    .agg(
        F.round(F.sum("risk_score") / F.nullif(F.sum("policy_count"), F.lit(0)), 3).alias("risk_score_to_policy_count_ratio"),
        F.max("policy_count").alias("max_policy_count"),
        F.sum("risk_score").alias("sum_risk_score_check"),
    )
    .filter(F.col("sum_risk_score_check") > 0.2)
    .drop("sum_risk_score_check")
)

without_matches = (
    summary
    .crossJoin(summary.agg(F.max("max_policy_count").alias("overall_max_max_policy_count")))
    .filter(F.col("max_policy_count") < F.col("overall_max_max_policy_count"))
    .drop("overall_max_max_policy_count")
)

result = without_matches.select("region", "max_policy_count")
