from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_or_unknown_by_reques = Window.partitionBy("channel_or_unknown").orderBy("requester_id", "id").rowsBetween(-3, Window.currentRow)
w_channel_ticket_count = Window.partitionBy("channel", "ticket_count")

filtered = (
    df_tickets
    .filter((F.col("csat").between(2, 5)) & (F.size(F.col("tags")) >= 1))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

profiled = (
    filtered.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_counts = (
    profiled.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

enriched = (
    with_counts
    .select(
        "*",
        F.when(F.col("channel").isin("chat", "email"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "channel_slug",
        "channel_or_unknown",
        F.sum("ticket_count").over(w_channel_or_unknown_by_reques).alias("ticket_count_sum_4"),
        F.count(F.lit(1)).over(w_channel_ticket_count).alias("rows_in_channel_ticket_count"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.count(F.when(F.col("event_type") == "resolved", 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    with_trends.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "channel_slug",
        "channel_or_unknown",
        "ticket_count_sum_4",
        "event_count_2",
        "event_event_type_count",
        F.date_trunc("quarter", F.col("closed_at")).alias("closed_at_quarter"),
    )
    .orderBy(F.col("channel_or_unknown").asc_nulls_last(), "channel")
)
