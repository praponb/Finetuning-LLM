from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter((F.col("actor").isin("customer", "agent")) & (F.hour(F.col("event_at")).between(10, 14)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

with_activity = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.opened_at")) < F.lit("2025-02-12").cast("date")), "left_semi")
)

joined_ticket = (
    with_activity.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel").isin("chat", "email")), "inner")
    .filter(F.array_contains(F.col("t.tags"), "upgrade"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.tags").alias("ticket_tags"),
    )
)

enriched = (
    joined_ticket
    .filter(F.size(F.col("ticket_tags")) >= 2)
    .select("*", F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"))
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_tags",
        "event_at_dayofweek",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
    .orderBy("event_at", F.col("actor").asc())
)
