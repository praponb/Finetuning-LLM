from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_to_date = Window.partitionBy(F.to_date("opened_at"))
w_agent_id_opened_at_to_date_b = Window.partitionBy("agent_id", F.to_date("opened_at")).orderBy("requester_id")
w_ticket_count_by_csat_vs_open = Window.partitionBy("ticket_count").orderBy("csat_vs_opened_at_to_date_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_count_by_csat_vs_open_2 = Window.partitionBy("ticket_count").orderBy("csat_vs_opened_at_to_date_avg")
w_requester_id_dense_rank_by_r = Window.partitionBy("requester_id_dense_rank").orderBy("requester_id_share_of_opened_at_to_date", "id")
w_ticket_count_by_csat_vs_open_3 = Window.partitionBy("ticket_count").orderBy("csat_vs_opened_at_to_date_avg", "id")

filtered_tickets = (
    df_tickets
    .filter((F.col("csat") < 5) & (F.col("closed_at") < F.lit("2025-02-19 21:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_windows = (
    filtered_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_opened_at_to_date), F.lit(0)), 4).alias("requester_id_share_of_opened_at_to_date"),
        F.round(F.col("csat") - F.avg("csat").over(w_opened_at_to_date), 2).alias("csat_vs_opened_at_to_date_avg"),
        F.min("csat").over(w_opened_at_to_date).alias("opened_at_to_date_min_csat"),
        F.min("requester_id").over(w_opened_at_to_date).alias("opened_at_to_date_min_requester_id"),
        F.dense_rank().over(w_agent_id_opened_at_to_date_b).alias("requester_id_dense_rank"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.min("requester_id").alias("min_ticket_requester_id"),
    )
)

with_stats = (
    with_windows.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.min_ticket_requester_id",
    )
)

with_windows_2 = (
    with_stats
    .select(
        "*",
        F.last("requester_id_dense_rank").over(w_ticket_count_by_csat_vs_open).alias("last_requester_id_dense_rank"),
        F.rank().over(w_ticket_count_by_csat_vs_open_2).alias("csat_vs_opened_at_to_date_avg_rank"),
        F.rank().over(w_requester_id_dense_rank_by_r).alias("requester_id_share_of_opened_at_to_date_rank"),
        F.row_number().over(w_ticket_count_by_csat_vs_open_3).alias("csat_vs_opened_at_to_date_avg_seq"),
        F.ntile(3).over(w_ticket_count_by_csat_vs_open_3).alias("csat_vs_opened_at_to_date_avg_tercile"),
    )
)

result = (
    with_windows_2
    .cube("channel", "csat_vs_opened_at_to_date_avg_tercile", "priority")
    .agg(
        F.grouping("channel").alias("is_total_channel"),
        F.count("*").alias("cnt"),
        F.sum("opened_at_to_date_min_requester_id").alias("total_opened_at_to_date_min_requester_id"),
    )
)
