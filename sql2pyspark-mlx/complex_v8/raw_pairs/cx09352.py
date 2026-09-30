from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_ticket_count_2 = Window.partitionBy("channel", "ticket_count_2")
w_all_by_ticket_count_prior_2 = Window.orderBy(F.col("ticket_count").desc(), "id").rowsBetween(-2, -1)
w_all_by_ticket_count_ties = Window.orderBy(F.col("ticket_count").desc())
w_all_by_ticket_count = Window.orderBy(F.col("ticket_count").desc(), "id")

filtered_tickets = (
    df_tickets
    .filter(F.col("agent_id") <= 9)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
        F.abs(F.col("requester_id") - 9).alias("requester_id_distance"),
        F.date_trunc("quarter", F.col("opened_at")).alias("opened_at_quarter"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.count("*").alias("ticket_count"),
        F.count("*").alias("ticket_count_2"),
    )
)

with_stats = (
    filtered_tickets.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.last_ticket_closed_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
    )
)

windowed = (
    with_stats
    .select(
        "id",
        "requester_id",
        "priority",
        "channel",
        "opened_at_quarter",
        "last_ticket_closed_at",
        "ticket_count_2",
        F.size(F.collect_set("agent_id").over(w_channel_ticket_count_2)).alias("distinct_agent_id_in_channel_ticket_count_2"),
        F.max("ticket_count_2").over(w_all_by_ticket_count_prior_2).alias("ticket_count_2_max_2_prior"),
        F.rank().over(w_all_by_ticket_count_ties).alias("ticket_count_rank"),
        F.round((F.unix_timestamp("opened_at_quarter") - F.unix_timestamp(F.lag("opened_at_quarter").over(w_all_by_ticket_count))) / 60, 1).alias("minutes_since_prev_opened_at_quarter"),
        F.min("ticket_count").over(w_channel_ticket_count_2).alias("channel_ticket_count_2_min_ticket_count"),
        F.round(F.avg("ticket_count_2").over(w_channel_ticket_count_2), 2).alias("channel_ticket_count_2_avg_ticket_count_2"),
    )
)

result = (
    windowed
    .groupBy("opened_at_quarter", "ticket_count_2")
    .pivot("priority", ["P4", "P3"])
    .agg(F.sum("channel_ticket_count_2_avg_ticket_count_2"))
    .orderBy("opened_at_quarter", "ticket_count_2")
)
