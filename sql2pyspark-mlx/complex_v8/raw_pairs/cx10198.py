from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
    )
)

tagged = (
    valid_events
    .select(
        "*",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
)

derived = (
    tagged
    .filter(F.col("actor_len") >= 4)
    .withColumn("days_since_event_at", F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")))
)

with_parent_ticket = (
    derived.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.requester_id").between(2, 19))
    .select(
        "s.*",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

result = (
    with_parent_ticket
    .select(
        "id",
        "event_type",
        "event_at_year",
        "days_since_event_at",
        "ticket_closed_at",
        "ticket_requester_id",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at_year")).alias("days_since_event_at_year"),
        F.round(F.col("actor_len") / F.nullif(F.col("ticket_requester_id"), F.lit(0)), 3).alias("actor_len_per_ticket_requester_id"),
    )
)
