from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_id = Window.partitionBy("channel", "agent_id")

recent = (
    df_tickets
    .filter(F.col("csat") <= 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "tags",
        F.when(F.col("priority").isin("P4", "P3"), "group_a").when(F.col("priority") == "P2", "group_b").otherwise("other").alias("priority_group"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.size(F.collect_set("priority_group").over(w_channel_agent_id)).alias("distinct_priority_group_in_channel_agent_id"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel_agent_id), F.lit(0)), 4).alias("requester_id_share_of_channel_agent_id"),
    )
)

with_parent_agent = (
    ranked.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.tags",
        "s.priority_group",
        "s.requester_id_share_of_channel_agent_id",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
        F.count(F.when(~F.col("event_type").isin("created", "escalated"), 1)).alias("event_event_type_count"),
    )
)

with_stats = (
    with_parent_agent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.last_event_at_2",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

result = (
    with_stats
    .groupBy("last_event_at_2")
    .pivot("priority_group", ["other", "group_b", "group_a"])
    .agg(F.count("requester_id"))
    .orderBy("last_event_at_2")
)
