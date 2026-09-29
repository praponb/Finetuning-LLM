from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter(F.col("actor").isin("system", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

summary = (
    valid_events
    .groupBy(
        "actor",
        F.date_format("event_at", "yyyy-MM").alias("event_at_ym"),
        "ticket_id",
    )
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.round(100 * F.count(F.when(F.col("actor") == "agent", 1)) / F.count("*"), 1).alias("pct_actor_agent"),
        F.count("*").alias("cnt"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.min("event_at").alias("first_event_at"),
    )
    .filter(F.col("cnt") >= 2)
)

scored = (
    summary
    .filter(F.col("pct_actor_agent") <= 60.0)
    .select(
        "*",
        F.date_trunc("month", F.col("first_event_at")).alias("first_event_at_month"),
        (F.col("distinct_actors") - F.col("distinct_event_types")).alias("distinct_actors_minus_distinct_event_types"),
    )
)

combined_details_ticket = (
    scored.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.actor",
        "s.event_at_ym",
        "s.ticket_id",
        "s.distinct_event_types",
        "s.pct_actor_agent",
        "s.cnt",
        "s.distinct_actors",
        "s.first_event_at",
        "s.distinct_actors_minus_distinct_event_types",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

joined_ticket = (
    combined_details_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.actor",
        "s.event_at_ym",
        "s.ticket_id",
        "s.distinct_event_types",
        "s.cnt",
        "s.distinct_actors",
        "s.first_event_at",
        "s.distinct_actors_minus_distinct_event_types",
        "s.ticket_opened_at",
        "s.ticket_tags",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

line_level = (
    joined_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.actor",
        "s.event_at_ym",
        "s.ticket_id",
        "s.distinct_event_types",
        "s.distinct_actors_minus_distinct_event_types",
        "s.ticket_closed_at",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

result = (
    line_level
    .select(
        "actor",
        "event_at_ym",
        "distinct_event_types",
        "distinct_actors_minus_distinct_event_types",
        "ticket_closed_at",
        "event_id",
        "event_actor",
        F.substring(F.trim(F.col("event_at_ym")), 1, 2).alias("event_at_ym_prefix"),
        F.substring(F.trim(F.col("event_actor")), 1, 4).alias("event_actor_prefix"),
    )
    .orderBy(F.col("event_id").desc())
)
