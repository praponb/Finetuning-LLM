from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

qualified = (
    scoped_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") != "reopened") & (F.col("t.event_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp")), "left_anti")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

summarized_rows = (
    qualified.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

ticket_stats_2 = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count_2"),
        F.count("*").alias("ticket_count_3"),
    )
)

with_stats = (
    summarized_rows.alias("s")
    .join(ticket_stats_2.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
        F.coalesce(F.col("t.ticket_count_3"), F.lit(0)).alias("ticket_count_3"),
    )
)

with_activity = (
    with_stats.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "reopened") & (F.col("t.actor").isin("customer", "system")), "left_anti")
)

result = (
    with_activity
    .filter(F.col("agent_id") >= 9)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "ticket_count_3",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
    .orderBy(F.col("agent_id").asc())
)
