from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("priority").isin("P3", "P1"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
    )
)

with_parent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.priority_group",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

annotated = (
    with_parent
    .select(
        "*",
        F.concat(F.col("priority_group"), F.lit("#"), F.col("id").cast("string")).alias("priority_group_ref"),
        F.lower(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
    )
)

result = (
    annotated
    .select(
        "id",
        "agent_id",
        "priority_group",
        "agent_name",
        "agent_team",
        "agent_name_clean",
        F.lower(F.regexp_replace(F.trim(F.col("agent_name_clean")), " ", "_")).alias("agent_name_clean_slug"),
    )
    .orderBy("priority_group", F.col("agent_id").desc())
)
