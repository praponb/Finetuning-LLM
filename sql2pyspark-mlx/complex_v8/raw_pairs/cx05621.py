from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter(F.col("csat") > 3)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("id").cast("string")).alias("priority_ref"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
    )
)

summarized_rows = (
    valid_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at", "t.last_event_at")
)

line_level_events = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.first_event_at",
        "s.last_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

enriched_event = (
    line_level_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.id") == F.col("s.event_id")) & (F.col("t.actor") == "agent"), "inner")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("t.ticket_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.first_event_at",
        "s.event_id",
        "s.event_actor",
        "s.event_type",
        F.col("t.event_type").alias("event_event_type"),
        "t.event_at",
        F.col("ti.priority").alias("ticket_priority"),
    )
)

screened = (
    enriched_event.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.requester_id") < 17) & (F.col("t.priority").isin("P1", "P2")), "left_semi")
)

derived = (
    screened
    .filter(F.col("ticket_priority").isNotNull())
    .select(
        "*",
        F.when(F.col("ticket_priority").isin("P4", "P1"), "group_a").when(F.col("ticket_priority") == "P2", "group_b").otherwise("other").alias("ticket_priority_group"),
        F.upper(F.trim(F.col("ticket_priority"))).alias("ticket_priority_clean"),
    )
)

result = (
    derived
    .select(
        "id",
        "opened_at",
        "closed_at",
        "first_event_at",
        "event_id",
        "event_actor",
        "event_type",
        "event_at",
        "ticket_priority_group",
        "ticket_priority_clean",
        F.initcap(F.trim(F.col("ticket_priority_group"))).alias("ticket_priority_group_clean"),
    )
    .orderBy("opened_at")
)
