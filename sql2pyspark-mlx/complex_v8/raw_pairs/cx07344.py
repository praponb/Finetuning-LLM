from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter((F.col("actor") == "agent") & (F.col("ticket_id") >= 59))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

qualified = (
    recent_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "agent") & (F.col("t.event_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")), "left_semi")
)

line_level_events = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "reopened"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.actor_event_type_label",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

with_details = (
    line_level_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

exploded_detail = (
    with_details.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_id",
        "s.event_event_type",
        "s.event_actor",
        "s.ticket_tags",
        "s.agent_hired_on",
        F.col("t.id").alias("event_id_2"),
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.event_type").alias("event_event_type_2"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_id",
        "event_actor",
        "ticket_tags",
        "agent_hired_on",
        "event_id_2",
        "event_actor_2",
        "event_event_type_2",
    )
    .orderBy("event_actor")
)
