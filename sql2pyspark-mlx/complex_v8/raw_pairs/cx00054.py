from pyspark.sql import functions as F

recent = (
    df_policyholders
    .filter(F.lower(F.col("holder_name")).like("%gr%"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.min("end_date").alias("first_policy_end_date"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.first_policy_end_date",
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["policy_count", "segment"], ["policy_count"], ["segment"]],
        "policy_count", "segment",
    )
    .agg(
        F.grouping("policy_count").alias("is_total_policy_count"),
        F.count(F.when(F.upper(F.trim(F.col("holder_name"))).like("A%"), 1)).alias("n_upper_trim_holder_name_like"),
        F.round(F.stddev("policy_count"), 2).alias("stddev_policy_count"),
    )
)
