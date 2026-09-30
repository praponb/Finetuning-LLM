from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

labeled = (
    valid
    .filter(F.col("ticket_id") <= 37)
    .select(
        "id",
        "ticket_id",
        "actor_or_unknown",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

rollup_base = (
    labeled
    .filter(F.col("event_at_label").isNotNull())
    .groupBy("ticket_id")
    .agg(
        F.min("event_at_month").alias("first_event_at_month"),
        F.max("event_at_month").alias("last_event_at_month"),
    )
)

annotated = (
    rollup_base
    .filter(F.hour(F.col("last_event_at_month")).between(6, 9))
    .select(
        "*",
        F.date_format(F.col("last_event_at_month"), "yyyy-MM-dd").alias("last_event_at_month_label"),
        F.round((F.unix_timestamp(F.col("first_event_at_month")) - F.unix_timestamp(F.col("last_event_at_month"))) / 3600, 1).alias("hours_last_event_at_month_to_first_event_at_mont"),
    )
)

enriched = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") < F.lit("2025-02-24 15:00:00").cast("timestamp")), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.ticket_id",
        "s.first_event_at_month",
        "s.last_event_at_month_label",
        "s.hours_last_event_at_month_to_first_event_at_mont",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    enriched
    .groupBy("ticket_csat")
    .pivot("ticket_channel", ["email", "web"])
    .agg(F.max("hours_last_event_at_month_to_first_event_at_mont"))
)
