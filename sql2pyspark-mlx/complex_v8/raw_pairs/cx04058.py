from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(7, 11)) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

combined_details = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.csat") < 4)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        F.col("t.csat").alias("ticket_csat"),
        "su.agent_name",
    )
)

scored = (
    combined_details
    .filter(F.col("ticket_id") <= 48)
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at_2"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    scored.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

result = (
    with_counts
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "days_since_event_at",
        "ticket_csat",
        "days_since_event_at_2",
        "event_at_month",
        "event_at_plus_30m",
        "last_event_at",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_csat").cast("string")).alias("event_type_ref"),
    )
    .orderBy("days_since_event_at_2")
)
