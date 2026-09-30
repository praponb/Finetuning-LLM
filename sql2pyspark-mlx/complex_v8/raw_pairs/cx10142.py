from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.col("priority") == "P4")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

tagged = (
    base
    .filter(F.array_contains(F.col("tags"), "outage"))
    .select(
        "*",
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
        F.length(F.trim(F.col("priority"))).alias("priority_len_2"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_counts = (
    tagged.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

joined = (
    with_counts.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.priority_len",
        "s.priority_len_2",
        "s.ticket_count",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    joined
    .select(
        "id",
        "requester_id",
        "priority",
        "csat",
        "tags",
        "priority_len",
        "priority_len_2",
        "ticket_count",
        "agent_team",
        "agent_name",
        "agent_hired_on",
    )
    .orderBy(F.col("priority_len_2").desc())
)
