from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

labeled = (
    valid_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_type_actor_label",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

enriched = (
    labeled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type_actor_label",
        "s.actor_slug",
        "s.actor_ref",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_type_actor_label",
        "actor_slug",
        "ticket_closed_at",
        F.date_trunc("day", F.col("ticket_closed_at")).alias("ticket_closed_at_day"),
    )
)
