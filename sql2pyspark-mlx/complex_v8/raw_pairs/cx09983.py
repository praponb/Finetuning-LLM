from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.when(F.col("status").isin("ACTIVE", "LAPSED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
    )
)

filtered = (
    filtered_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") > 2349.3) & (F.col("c.claimed_amount") >= 18364.1), "left_semi")
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("premium").alias("min_policy_premium"),
        F.sum("premium").alias("total_policy_premium"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.min_policy_premium", "p.total_policy_premium")
)

result = (
    summarized_rows
    .select("id", "total_policy_premium", "min_policy_premium")
    .unpivot(
        ids=["id"],
        values=["total_policy_premium", "min_policy_premium"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
