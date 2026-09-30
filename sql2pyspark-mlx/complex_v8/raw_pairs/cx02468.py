from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 14)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type_actor_label",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

with_details_ticket = (
    joined.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.*", F.col("t.opened_at").alias("ticket_opened_at"))
)

with_children = (
    with_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") != "system"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type_actor_label",
        "s.ticket_closed_at",
        "s.ticket_priority",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    with_children
    .cube("ticket_id", "event_type")
    .agg(
        F.sum(F.when(F.col("event_actor").isNotNull(), 1).otherwise(0)).alias("n_event_actor_is_not_null"),
        F.count(F.when(F.col("ticket_priority").isNotNull(), 1)).alias("n_ticket_priority_is_not_null"),
        F.min("event_at").alias("first_event_at"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
