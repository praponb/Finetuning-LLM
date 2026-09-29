from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_count_full = Window.orderBy("ticket_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id = Window.partitionBy("agent_id")
w_all_by_ticket_count = Window.orderBy("ticket_count", "id")

valid = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "login"))
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
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count("*").alias("ticket_count_2"),
        F.max("requester_id").alias("max_ticket_requester_id"),
    )
)

with_counts = (
    valid.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
        "t.max_ticket_requester_id",
    )
)

metrics = (
    with_counts
    .select(
        "*",
        F.last("ticket_count_2").over(w_all_by_ticket_count_full).alias("last_ticket_count_2"),
        F.sum("requester_id").over(w_agent_id).alias("agent_id_sum_requester_id"),
        F.lag("ticket_count_2", 1, 0).over(w_all_by_ticket_count).alias("prev_ticket_count_2"),
        F.round(F.percent_rank().over(w_all_by_ticket_count), 3).alias("ticket_count_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        "max_ticket_requester_id",
        "last_ticket_count_2",
        "agent_id_sum_requester_id",
        "ticket_count_percent_rank",
        F.date_trunc("hour", F.col("opened_at")).alias("opened_at_hour"),
    )
    .orderBy(F.col("channel").asc(), "id")
    .limit(20)
)
