from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "tags",
    )
)

priority_stats = (
    cleaned
    .groupBy("priority")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

vs_group = (
    cleaned.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

with_parent_agent = (
    vs_group.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.tags",
        "s.avg_requester_id",
        "s.requester_id_minus_avg",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

combined_details_agent = (
    with_parent_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.tags",
        "s.avg_requester_id",
        "s.agent_hired_on",
        "s.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    combined_details_agent
    .groupBy("agent_hired_on")
    .pivot("agent_tier", ["T3", "T1"])
    .agg(F.sum("requester_id"))
)
