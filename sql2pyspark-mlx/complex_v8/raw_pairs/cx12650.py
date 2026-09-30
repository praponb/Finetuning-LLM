from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type").isin("replied", "resolved", "reopened")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

qualified = (
    valid
    .crossJoin(valid.agg(F.max("days_since_event_at").alias("overall_max_days_since_event_at")))
    .filter(F.col("days_since_event_at") < F.col("overall_max_days_since_event_at"))
    .drop("overall_max_days_since_event_at")
)

with_details_ticket = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_hour",
        "s.event_at_label",
        "s.days_since_event_at",
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

actor_stats = (
    with_details_ticket
    .groupBy("actor")
    .agg(F.max("days_since_event_at").alias("max_days_since_event_at"))
)

outliers = (
    with_details_ticket.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.days_since_event_at") == F.col("g.max_days_since_event_at"))
    .select(
        "s.*",
        "g.max_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.max_days_since_event_at"), 2).alias("days_since_event_at_minus_max"),
    )
)

result = (
    outliers
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "days_since_event_at",
        "ticket_agent_id",
        "days_since_event_at_minus_max",
    )
    .orderBy(F.col("ticket_id").desc())
)
