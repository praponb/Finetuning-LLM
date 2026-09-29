from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
    )
)

with_activity = (
    filtered_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.hour(F.col("t.closed_at")).between(9, 13)) & (F.col("t.opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "left_anti")
)

with_parent_agent = (
    with_activity.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

with_parent_agent_2 = (
    with_parent_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.agent_name",
        "s.agent_team",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_parent_agent_3 = (
    with_parent_agent_2.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.year(F.col("su.hired_on")) == 2024), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.agent_name",
        "s.agent_hired_on",
        F.col("su.hired_on").alias("agent_hired_on_2"),
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

result = (
    with_parent_agent_3
    .select(
        "id",
        "closed_at",
        "csat",
        "agent_name",
        "agent_hired_on",
        "agent_hired_on_2",
        "agent_agent_name",
        F.concat(F.col("agent_name"), F.lit("#"), F.col("csat").cast("string")).alias("agent_name_ref"),
    )
    .orderBy(F.col("agent_hired_on_2").asc(), "id")
    .limit(20)
)
