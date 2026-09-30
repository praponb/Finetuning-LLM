from pyspark.sql import functions as F

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

with_children_events = (
    joined_ranges.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

annotated = (
    with_children_events
    .filter(F.month(F.col("event_event_at")).isin(1, 3))
    .select(
        "*",
        F.upper(F.trim(F.col("event_event_type"))).alias("event_event_type_clean"),
        F.coalesce(F.col("event_actor"), F.lit("UNKNOWN")).alias("event_actor_or_unknown"),
    )
)

result = (
    annotated
    .select(
        "id",
        "event_id",
        "event_actor",
        "event_event_type",
        "event_event_type_clean",
    )
)
