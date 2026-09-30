from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_csat_by_csat = Window.partitionBy("priority", "csat").orderBy(F.col("csat").desc(), "id")
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_csat_by_csat_ties = Window.partitionBy("priority", "csat").orderBy(F.col("csat").desc())

filtered = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.lag("csat").over(w_priority_csat_by_csat).alias("prev_csat"),
        F.nth_value("closed_at", 2).over(w_all_by_opened_at_full).alias("second_closed_at"),
        F.round(F.percent_rank().over(w_priority_csat_by_csat_ties), 3).alias("csat_percent_rank"),
    )
)

channel_stats = (
    windowed
    .groupBy("channel")
    .agg(F.avg("prev_csat").alias("avg_prev_csat"))
)

compared = (
    windowed.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.prev_csat") > F.col("g.avg_prev_csat"))
    .select(
        "s.*",
        "g.avg_prev_csat",
        F.round(F.col("s.prev_csat") - F.col("g.avg_prev_csat"), 2).alias("prev_csat_minus_avg"),
    )
)

with_children_events = (
    compared.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.prev_csat",
        "s.csat_percent_rank",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
        "t.event_at",
    )
)

result = (
    with_children_events
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "prev_csat",
        "csat_percent_rank",
        "event_id",
        "event_actor",
        "event_at",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.abs(F.col("csat_percent_rank") - 0.3).alias("csat_percent_rank_distance"),
    )
    .orderBy(F.col("event_at").asc_nulls_last())
)
