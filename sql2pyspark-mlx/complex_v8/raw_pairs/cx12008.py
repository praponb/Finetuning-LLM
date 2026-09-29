from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        F.when(F.col("requester_id") >= 19, "A").when(F.col("requester_id") >= 7, "B").otherwise("C").alias("requester_id_tier"),
        F.lower(F.trim(F.col("channel"))).alias("channel_clean"),
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "agent") & (F.col("t.event_type").isin("reopened", "replied")), "left_semi")
    .filter(F.upper(F.trim(F.col("channel_clean"))).like("W%"))
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
        F.max("event_at").alias("last_event_at_3"),
    )
)

summarized_rows = (
    without_matches.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.last_event_at_2",
        "t.last_event_at_3",
    )
)

result = (
    summarized_rows
    .groupBy("last_event_at_3")
    .pivot("channel", ["phone", "chat", "web", "email"])
    .agg(F.max("csat"))
)
