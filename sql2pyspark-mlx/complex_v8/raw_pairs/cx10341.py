from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter(~F.array_contains(F.col("tags"), "outage"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.agent_name").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.tags",
        "su.agent_name",
    )
)

qualified = (
    combined_details.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.agent_name").isNotNull()) & (~F.col("su.team").isin("Billing", "Tech")), "left_semi")
)

result = (
    qualified
    .rollup("agent_id", "priority")
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.count(F.when(F.col("priority") == "P1", 1)).alias("n_priority_p1"),
    )
)
