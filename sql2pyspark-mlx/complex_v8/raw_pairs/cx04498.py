from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter(F.col("channel") == "chat")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
        F.count(F.when(F.col("actor") == "customer", 1)).alias("event_actor_count"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.first_event_at_2",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

with_details = (
    summarized_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.tags",
        "s.first_event_at",
        "s.first_event_at_2",
        "s.event_actor_count",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

flattened = (
    with_details
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "first_event_at",
        "first_event_at_2",
        "event_actor_count",
        "agent_team",
        "agent_tier",
        "agent_name",
        F.explode("tags").alias("tag"),
    )
)

result = (
    flattened
    .cube("agent_team", "tag", "agent_tier")
    .agg(F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"))
    .orderBy(
        F.col("agent_team").asc_nulls_last(),
        F.col("tag").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
