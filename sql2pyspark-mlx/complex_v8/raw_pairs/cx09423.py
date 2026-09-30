from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)

enriched = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_len",
        "s.event_at_year",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.tier").alias("agent_tier"),
    )
)

per_group = (
    enriched
    .filter(F.col("ticket_opened_at") >= F.lit("2025-02-06 06:00:00").cast("timestamp"))
    .groupBy("ticket_id", "agent_tier")
    .agg(F.min("actor_len").alias("min_actor_len"), F.count("*").alias("cnt"))
)

derived = (
    per_group
    .withColumn("min_actor_len_per_cnt", F.round(F.col("min_actor_len") / F.nullif(F.col("cnt"), F.lit(0)), 3))
)

combined_details_ticket = (
    derived.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.hour(F.col("t.opened_at")).between(7, 9))
    .select(
        "s.ticket_id",
        "s.agent_tier",
        "s.min_actor_len",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.csat").alias("ticket_csat"),
        "su.agent_name",
    )
)

with_parent = (
    combined_details_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.ticket_id",
        "s.agent_tier",
        "s.min_actor_len",
        "s.ticket_closed_at",
        F.col("t.csat").alias("ticket_csat_2"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    with_parent
    .select(
        "ticket_id",
        "agent_tier",
        "min_actor_len",
        "ticket_closed_at",
        "ticket_csat_2",
        F.substring(F.trim(F.col("ticket_channel")), 1, 4).alias("ticket_channel_prefix"),
        F.date_trunc("month", F.col("ticket_closed_at")).alias("ticket_closed_at_month"),
    )
)
