from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_id_channel_by_tags_cou = Window.partitionBy("event_id", "channel").orderBy(F.col("tags_count").desc(), "id", "event_id")
w_agent_id = Window.partitionBy("agent_id")

filtered = (
    df_tickets
    .filter((F.col("channel") == "web") & (F.col("agent_id") >= 7))
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
        F.concat_ws("|", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        F.size(F.col("tags")).alias("tags_count"),
    )
)

with_children = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.tags_count",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

sequenced = (
    with_children
    .select(
        "*",
        F.lead("csat").over(w_event_id_channel_by_tags_cou).alias("next_csat"),
        F.round(F.col("tags_count") - F.avg("tags_count").over(w_agent_id), 2).alias("tags_count_vs_agent_id_avg"),
        F.min("csat").over(w_agent_id).alias("agent_id_min_csat"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_counts = (
    sequenced.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "tags_count",
        "event_id",
        "event_type",
        "next_csat",
        "tags_count_vs_agent_id_avg",
        "ticket_count",
    )
    .orderBy("priority")
)
