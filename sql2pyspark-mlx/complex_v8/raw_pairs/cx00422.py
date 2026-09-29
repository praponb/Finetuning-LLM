from pyspark.sql import functions as F

valid_events = df_ticket_events.filter(F.col("ticket_id") >= 33)

with_parent = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") > 4), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

enriched = (
    with_parent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.ticket_tags",
        "s.ticket_closed_at",
        F.col("t.tags").alias("ticket_tags_2"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

exploded_detail = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.ticket_closed_at",
        "s.ticket_tags_2",
        "s.ticket_requester_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

per_group = (
    exploded_detail
    .filter(F.col("ticket_id") >= 33)
    .groupBy(
        F.date_trunc("month", "event_event_at").alias("event_event_at_month"),
        F.date_format("ticket_closed_at", "yyyy-MM").alias("ticket_closed_at_ym"),
    )
    .agg(
        F.sum(F.when(F.col("event_actor") == "customer", F.col("ticket_requester_id"))).alias("ticket_requester_id_event_actor_customer"),
        F.round(100 * F.count(F.when(F.col("event_type") == "reopened", 1)) / F.count("*"), 1).alias("pct_event_type_reopened"),
    )
)

result = (
    per_group
    .select(
        "event_event_at_month",
        "ticket_closed_at_ym",
        "ticket_requester_id_event_actor_customer",
        F.lower(F.regexp_replace(F.trim(F.col("ticket_closed_at_ym")), " ", "_")).alias("ticket_closed_at_ym_slug"),
        F.substring(F.trim(F.col("ticket_closed_at_ym")), 1, 2).alias("ticket_closed_at_ym_prefix"),
    )
    .orderBy(F.col("ticket_closed_at_ym_slug").desc())
)
