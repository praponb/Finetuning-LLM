from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_channel = Window.partitionBy("csat", "channel")
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_closed_at_month_by_closed_at = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy("closed_at")
w_channel_closed_at_to_date_by = Window.partitionBy("channel", F.to_date("closed_at")).orderBy("opened_at", "id")
w_channel_agent_id = Window.partitionBy("channel", "agent_id")
w_agent_id_closed_at_month = Window.partitionBy("agent_id", F.month("closed_at"))
w_channel_closed_at_to_date_by_2 = Window.partitionBy("channel", F.to_date("closed_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_closed_at_to_date_by_3 = Window.partitionBy("channel", F.to_date("closed_at")).orderBy("opened_at")
w_closed_at_month_by_closed_at_2 = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_tickets = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-02-24 09:00:00").cast("timestamp")) & (F.col("priority") == "P3"))
)

with_trends = (
    filtered_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.max("csat").over(w_csat_channel).alias("csat_channel_max_csat"),
        F.lead("requester_id").over(w_agent_id_by_opened_at).alias("next_requester_id"),
        F.dense_rank().over(w_closed_at_month_by_closed_at).alias("closed_at_dense_rank"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_csat_channel), F.lit(0)), 4).alias("requester_id_share_of_csat_channel"),
        (F.col("csat") - F.lag("csat").over(w_channel_closed_at_to_date_by)).alias("csat_change"),
        F.count(F.lit(1)).over(w_channel_agent_id).alias("rows_in_channel_agent_id"),
        F.max("csat").over(w_agent_id_closed_at_month).alias("agent_id_closed_at_month_max_csat"),
        F.sum("csat").over(w_channel_closed_at_to_date_by_2).alias("running_csat"),
        F.min("csat").over(w_csat_channel).alias("csat_channel_min_csat"),
        F.min("requester_id").over(w_channel_closed_at_to_date_by_2).alias("min_requester_id_so_far"),
        F.rank().over(w_channel_closed_at_to_date_by_3).alias("opened_at_rank"),
        F.ntile(10).over(w_agent_id_by_opened_at).alias("opened_at_decile"),
        F.rank().over(w_agent_id_by_opened_at).alias("opened_at_rank_2"),
        F.round(F.cume_dist().over(w_agent_id_by_opened_at), 3).alias("opened_at_cume_dist"),
        F.last("closed_at").over(w_closed_at_month_by_closed_at_2).alias("last_closed_at"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat_channel), 2).alias("csat_vs_csat_channel_avg"),
    )
)

derived = (
    with_trends
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.round(F.col("opened_at_cume_dist") * 1.5, 2).alias("opened_at_cume_dist_adj"),
        (F.col("last_closed_at") + F.expr("INTERVAL 7 DAYS")).alias("last_closed_at_plus_7d"),
    )
)

result = (
    derived
    .filter(F.col("opened_at_rank") > 4)
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        "csat_channel_max_csat",
        "requester_id_share_of_csat_channel",
        "csat_change",
        "rows_in_channel_agent_id",
        "agent_id_closed_at_month_max_csat",
        "csat_channel_min_csat",
        "min_requester_id_so_far",
        "opened_at_decile",
        "opened_at_rank_2",
        "last_closed_at",
        "csat_vs_csat_channel_avg",
        "opened_at_cume_dist_adj",
        "last_closed_at_plus_7d",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.size(F.col("tags")).alias("tags_count"),
    )
    .orderBy(
        F.col("min_requester_id_so_far").desc(),
        F.col("opened_at_cume_dist_adj").asc(),
        "id",
    )
    .limit(20)
)
