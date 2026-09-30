from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter(F.col("csat") > 3)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_children_events = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.csat",
        "s.tags",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    with_children_events
    .groupingSets(
        [["csat", "agent_id"], ["csat"], ["agent_id"]],
        "csat", "agent_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("event_actor")).alias("event_actor_list"),
    )
    .orderBy(F.col("csat").asc_nulls_last(), F.col("agent_id").asc_nulls_last())
)
