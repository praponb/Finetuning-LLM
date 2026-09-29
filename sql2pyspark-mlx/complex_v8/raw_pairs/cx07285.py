from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 58)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

screened = (
    base.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-03-08").cast("date")) & (F.col("t.event_type") == "reopened"), "left_semi")
)

with_details_ticket = (
    screened.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_ref",
        "s.actor_event_type_label",
        "s.event_at_week",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        "su.agent_name",
    )
)

result = (
    with_details_ticket
    .rollup("ticket_csat", "actor")
    .agg(
        F.min("event_at_week").alias("first_event_at_week"),
        F.round(F.stddev("ticket_requester_id"), 2).alias("stddev_ticket_requester_id"),
    )
)
