from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
    )
)

with_children = (
    scoped_tickets.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

line_level_events = (
    with_children.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.event_id",
        "s.event_at",
        "s.event_type",
        F.col("t.id").alias("event_id_2"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    line_level_events
    .groupingSets(
        [["event_id_2", "event_actor"], ["event_id_2"], []],
        "event_id_2", "event_actor",
    )
    .agg(
        F.count(F.when(F.col("event_event_type") == "assigned", 1)).alias("n_event_event_type_assigned"),
        F.countDistinct("event_actor").alias("distinct_event_actors"),
        F.countDistinct("event_id").alias("distinct_event_ids"),
    )
)
