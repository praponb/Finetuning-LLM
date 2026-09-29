from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_all_by_opened_at_ties = Window.orderBy("opened_at")
w_channel_csat_by_opened_at_de = Window.partitionBy("channel", "csat").orderBy(F.col("opened_at_dense_rank_bucket").desc(), "id")
w_channel_csat_by_opened_at_de_2 = Window.partitionBy("channel", "csat").orderBy(F.col("opened_at_dense_rank_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_priority = Window.partitionBy("agent_id", "priority")

recent_tickets = (
    df_tickets
    .filter((F.month(F.col("opened_at")).isin(1, 3)) & (F.col("priority").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

windowed = (
    recent_tickets
    .select(
        "*",
        F.round(F.col("csat") - F.avg("csat").over(w_priority), 2).alias("csat_vs_priority_avg"),
        F.dense_rank().over(w_all_by_opened_at_ties).alias("opened_at_dense_rank"),
        F.sum("priority_len").over(w_priority).alias("priority_sum_priority_len"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
    )
)

summarized_rows = (
    windowed.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at", "t.last_event_at")
)

scored = (
    summarized_rows
    .select(
        "*",
        (F.floor(F.col("opened_at_dense_rank") / 2) * 2).alias("opened_at_dense_rank_bucket"),
        F.abs(F.col("csat") - 3).alias("csat_distance"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "priority_len",
        "priority_sum_priority_len",
        "first_event_at",
        "last_event_at",
        (F.col("opened_at_dense_rank_bucket") - F.lag("opened_at_dense_rank_bucket").over(w_channel_csat_by_opened_at_de)).alias("opened_at_dense_rank_bucket_change"),
        F.last("priority_len").over(w_channel_csat_by_opened_at_de_2).alias("last_priority_len"),
        F.round(F.col("csat_distance") - F.avg("csat_distance").over(w_agent_id_priority), 2).alias("csat_distance_vs_agent_id_priority_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "priority_len",
        "priority_sum_priority_len",
        "first_event_at",
        "opened_at_dense_rank_bucket_change",
        "last_priority_len",
    )
)
