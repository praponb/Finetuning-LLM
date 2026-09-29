from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

enriched = (
    prepared_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_date",
        F.year(F.col("event_at")).alias("event_at_year"),
        F.date_trunc("year", F.col("event_at_date")).alias("event_at_date_year"),
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.col("t.opened_at") < F.lit("2025-03-08 11:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_year",
        "s.event_at_date_year",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    joined
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_year",
        "event_at_date_year",
        "ticket_closed_at",
        "ticket_channel",
        "agent_tier",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.date_format(F.col("ticket_closed_at"), "yyyy-MM-dd").alias("ticket_closed_at_label"),
    )
    .orderBy(F.col("event_at_year").desc(), F.col("actor").asc())
)
