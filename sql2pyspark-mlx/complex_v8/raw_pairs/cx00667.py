from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.col("priority").isin("P3", "P4")) & (F.hour(F.col("opened_at")).between(7, 12)))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "tags",
    )
)

per_group = (
    recent
    .groupBy("priority", "agent_id", "channel")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("channel").isin("chat", "web"), 1)).alias("n_channel_in_chat_web"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.sum(F.when(F.col("channel") == "phone", 1).otherwise(0)).alias("n_channel_phone"),
        F.count(F.when(F.col("priority") == "P3", 1)).alias("n_priority_p3"),
    )
)

scored = (
    per_group
    .filter(F.col("n_channel_in_chat_web") < 8)
    .select(
        "*",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
        F.size(F.col("priority_list")).alias("priority_list_count"),
    )
)

enriched_agent = (
    scored.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.priority",
        "s.agent_id",
        "s.channel",
        "s.cnt",
        "s.priority_list",
        "s.n_channel_phone",
        "s.channel_prefix",
        "s.priority_list_count",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

result = (
    enriched_agent
    .select(
        "priority",
        "agent_id",
        "n_channel_phone",
        "priority_list_count",
    )
    .unpivot(
        ids=["priority", "agent_id"],
        values=["n_channel_phone", "priority_list_count"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("priority", "agent_id", "metric_name")
)
