from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label_2"),
    )
)

annotated = (
    scoped
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.abs(F.col("actor_len") - 7).alias("actor_len_distance"),
    )
)

with_parent_ticket = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_len",
        "s.actor_event_type_label",
        "s.days_since_event_at",
        "s.event_type_or_unknown",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    with_parent_ticket
    .filter(F.col("ticket_channel") == "web")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_len",
        "actor_event_type_label",
        "event_type_or_unknown",
        "ticket_priority",
        "ticket_closed_at",
        F.upper(F.trim(F.col("event_type_or_unknown"))).alias("event_type_or_unknown_clean"),
    )
    .orderBy(
        F.col("ticket_closed_at").asc_nulls_last(),
        F.col("event_type_or_unknown_clean").desc(),
        "id",
    )
    .limit(10)
)
