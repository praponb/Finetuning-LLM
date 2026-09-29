from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-12 11:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
    )
)

tagged = (
    filtered
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at_year"))) / 60, 1).alias("minutes_closed_at_year_to_opened_at"),
        F.month(F.col("opened_at")).alias("opened_at_month"),
    )
)

detail_rows = (
    tagged.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        "s.minutes_closed_at_year_to_opened_at",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

with_children_events = (
    detail_rows.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.hour(F.col("t.event_at")).between(8, 13)), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        "s.minutes_closed_at_year_to_opened_at",
        "s.event_id",
        "s.event_type",
        F.col("t.id").alias("event_id_2"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor_2"),
    )
)

result = (
    with_children_events
    .cube("event_actor_2", "event_id")
    .agg(F.count("*").alias("row_count"))
    .select(
        F.coalesce(F.col("event_actor_2"), F.lit("ALL")).alias("event_actor_2"),
        "event_id",
        "row_count",
    )
)
