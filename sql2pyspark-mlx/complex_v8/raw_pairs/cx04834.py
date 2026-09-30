from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter(F.hour(F.col("closed_at")).between(6, 11))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.concat_ws(": ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
    )
)

filtered = (
    recent_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")), "left_anti")
)

per_label = (
    filtered
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "priority_len",
        "priority_channel_label",
        "opened_at_date",
        F.posexplode("tags").alias("tag_pos", "tag"),
    )
)

with_children_events = (
    per_label.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.priority_len",
        "s.priority_channel_label",
        "s.opened_at_date",
        "s.tag_pos",
        "s.tag",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

result = (
    with_children_events
    .groupingSets(
        [["tag", "priority_len", "tag_pos"], ["tag"]],
        "tag", "priority_len", "tag_pos",
    )
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.countDistinct("tag").alias("distinct_tags"),
    )
    .select(
        F.coalesce(F.col("tag"), F.lit("ALL")).alias("tag"),
        "priority_len",
        "tag_pos",
        "first_event_at",
        "distinct_tags",
    )
    .orderBy(
        F.col("tag").asc_nulls_last(),
        F.col("priority_len").asc_nulls_last(),
        F.col("tag_pos").asc_nulls_last(),
    )
)
