from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.col("channel") == "email")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        (F.col("closed_at") + F.expr("INTERVAL 30 MINUTES")).alias("closed_at_plus_30m"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

without_matches = (
    scoped_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.month(F.col("t.event_at")).isin(2, 3)) & (F.col("t.actor") == "agent"), "left_anti")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.round(F.avg("csat"), 2).alias("avg_ticket_csat"))
)

with_stats = (
    without_matches.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.avg_ticket_csat")
)

result = (
    with_stats
    .select(
        "id",
        "requester_id",
        "csat",
        "closed_at_plus_30m",
        "days_since_closed_at",
        "requester_id_filled",
        "avg_ticket_csat",
        F.abs(F.col("days_since_closed_at") - 315).alias("days_since_closed_at_distance"),
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
    )
)
