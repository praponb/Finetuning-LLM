from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("reopened", "assigned", "replied")) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

combined_details_ticket = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
    )
)

with_activity = (
    combined_details_ticket.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
    .filter(F.col("ticket_id") <= 12)
)

combined_details = (
    with_activity.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") < 5), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.ticket_priority",
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.tier").alias("agent_tier"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

summarized_rows = (
    combined_details.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    summarized_rows
    .filter(F.col("event_type").isin("resolved", "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "ticket_priority",
        "ticket_csat",
        "agent_tier",
        "event_count",
        F.when(F.col("event_count") >= 6, "HIGH").when(F.col("event_count") >= 3, "MEDIUM").otherwise("LOW").alias("event_count_tier"),
        F.least(F.col("event_count"), F.col("ticket_csat")).alias("least_event_count_ticket_csat"),
    )
    .orderBy(F.col("ticket_id").desc())
)
