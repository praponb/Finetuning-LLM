from pyspark.sql import functions as F

cleaned = (
    df_policyholders
    .filter(F.col("segment") == "SME")
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
    )
)

filtered = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.premium") <= 3544.1), "left_anti")
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.count("*").alias("policy_count_2"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
    )
)

result = (
    with_stats
    .select("id", "policy_count", "policy_count_2")
    .unpivot(
        ids=["id"],
        values=["policy_count", "policy_count_2"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
