from pyspark.sql import functions as F

filtered = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        "s.premium_filled",
        "p.holder_name",
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("policy_status_count"),
    )
)

with_stats = (
    with_details.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
    )
)

result = (
    with_stats
    .cube("holder_id", "status")
    .agg(F.count("*").alias("n_rows"))
    .orderBy(F.col("holder_id").asc_nulls_last(), F.col("status").asc_nulls_last())
)
