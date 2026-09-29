from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter((F.col("priority") != "P2") & (F.col("opened_at") < F.lit("2025-02-24 14:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
        (F.col("closed_at") + F.expr("INTERVAL 2 HOURS")).alias("closed_at_plus_2h"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

profiled = (
    scoped.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at", "t.last_event_at_2")
)

result = (
    profiled
    .cube("channel", "priority")
    .agg(
        F.grouping("channel").alias("is_total_channel"),
        F.min("csat_plus_requester_id").alias("min_csat_plus_requester_id"),
        F.round(F.avg("csat_plus_requester_id"), 2).alias("avg_csat_plus_requester_id"),
    )
    .select(
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "is_total_channel",
        "min_csat_plus_requester_id",
        "avg_csat_plus_requester_id",
    )
    .orderBy(F.col("channel").asc_nulls_last(), F.col("priority").asc_nulls_last())
)
