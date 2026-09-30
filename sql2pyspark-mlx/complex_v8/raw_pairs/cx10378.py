from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_all_by_event_count_2 = Window.orderBy(F.col("event_count_2").asc_nulls_last(), "id")

recent_tickets = (
    df_tickets
    .filter(~F.col("channel").isin("phone", "chat"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

with_counts = (
    recent_tickets.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

derived = (
    with_counts
    .select(
        "*",
        F.abs(F.col("requester_id") - 13).alias("requester_id_distance"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count_2"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

summarized_rows = (
    derived.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
        "t.first_event_at_2",
    )
)

with_windows = (
    summarized_rows
    .select(
        "*",
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.round(F.col("requester_id_distance") / F.nullif(F.sum("requester_id_distance").over(w_agent_id), F.lit(0)), 4).alias("requester_id_distance_share_of_agent_id"),
        F.round(F.col("hours_opened_at_to_closed_at") - F.avg("hours_opened_at_to_closed_at").over(w_agent_id), 2).alias("hours_opened_at_to_closed_at_vs_agent_id_avg"),
        F.round(F.avg("days_since_closed_at").over(w_agent_id), 2).alias("agent_id_avg_days_since_closed_at"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_event_count_2))) / 60, 1).alias("minutes_since_prev_closed_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        "requester_id_per_csat",
        "channel_or_unknown",
        "hours_opened_at_to_closed_at",
        "event_count",
        "days_since_closed_at",
        "event_count_2",
        "first_event_at_2",
        "rows_in_agent_id",
        "requester_id_distance_share_of_agent_id",
        "agent_id_avg_days_since_closed_at",
        "minutes_since_prev_closed_at",
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
    )
)
