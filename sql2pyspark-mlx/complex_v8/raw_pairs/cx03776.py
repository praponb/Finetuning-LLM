from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_csat_full = Window.partitionBy("csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_csat = Window.partitionBy("csat").orderBy("csat", "id")
w_priority = Window.partitionBy("priority")

cleaned_tickets = (
    df_tickets
    .filter((F.col("requester_id") < 23) & (F.col("priority") == "P1"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
    )
)

with_windows = (
    cleaned_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        "channel_ref",
        "channel_prefix",
        "closed_at_hour",
        F.nth_value("closed_at_hour", 2).over(w_csat_by_csat_full).alias("second_closed_at_hour"),
        F.ntile(10).over(w_csat_by_csat).alias("csat_decile"),
        F.round(F.col("csat") - F.avg("csat").over(w_priority), 2).alias("csat_vs_priority_avg"),
        F.nth_value("csat", 2).over(w_csat_by_csat_full).alias("second_csat"),
        F.min("closed_at_hour").over(w_priority).alias("priority_min_closed_at_hour"),
    )
)

detail_rows = (
    with_windows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_ref",
        "s.channel_prefix",
        "s.csat_decile",
        "s.csat_vs_priority_avg",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

result = (
    detail_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "channel_ref",
        "csat_decile",
        "csat_vs_priority_avg",
        "event_id",
        "event_actor",
        F.size(F.col("tags")).alias("tags_count"),
        F.round(F.col("csat_vs_priority_avg") / F.nullif(F.col("csat_decile"), F.lit(0)), 3).alias("csat_vs_priority_avg_per_csat_decile"),
    )
    .orderBy("csat", F.col("csat_decile").desc())
)
