from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_to_date_by_opened = Window.partitionBy(F.to_date("opened_at")).orderBy("opened_at_plus_1d")
w_opened_at_plus_1d_month = Window.partitionBy(F.date_trunc("month", "opened_at_plus_1d"))
w_channel_opened_at_month_by_c = Window.partitionBy("channel", F.month("opened_at")).orderBy(F.col("csat_per_requester_id").desc(), "id").rowsBetween(-3, Window.currentRow)
w_channel_opened_at_month_by_c_2 = Window.partitionBy("channel", F.month("opened_at")).orderBy(F.col("csat_per_requester_id").desc(), "id")
w_opened_at_to_date_agent_id = Window.partitionBy(F.to_date("opened_at"), "agent_id")
w_all_by_csat_per_requester_id = Window.orderBy("csat_per_requester_id_rank", "id", "tag").rowsBetween(-2, -1)
w_all_by_csat_per_requester_id_2 = Window.orderBy("csat_per_requester_id_rank", "id", "tag")

cleaned = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "outage")) & (F.col("priority") == "P1"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        (F.col("opened_at") + F.expr("INTERVAL 1 DAYS")).alias("opened_at_plus_1d"),
    )
)

with_trends = (
    cleaned
    .select(
        "id",
        "agent_id",
        "opened_at",
        "tags",
        "priority_prefix",
        "opened_at_plus_1d",
        F.rank().over(w_opened_at_to_date_by_opened).alias("opened_at_plus_1d_rank"),
        F.round(F.avg("csat_per_requester_id").over(w_opened_at_plus_1d_month), 2).alias("opened_at_plus_1d_month_avg_csat_per_requester_i"),
        F.max("csat_per_requester_id").over(w_channel_opened_at_month_by_c).alias("csat_per_requester_id_max_4"),
        F.rank().over(w_channel_opened_at_month_by_c_2).alias("csat_per_requester_id_rank"),
    )
)

per_label = (
    with_trends
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority_prefix",
        "opened_at_plus_1d",
        "opened_at_plus_1d_rank",
        "opened_at_plus_1d_month_avg_csat_per_requester_i",
        "csat_per_requester_id_max_4",
        "csat_per_requester_id_rank",
        F.explode("tags").alias("tag"),
    )
)

windowed = (
    per_label
    .select(
        "*",
        F.sum("csat_per_requester_id_max_4").over(w_opened_at_to_date_agent_id).alias("opened_at_to_date_agent_id_sum_csat_per_requeste"),
        F.round(F.col("opened_at_plus_1d_rank") / F.nullif(F.sum("opened_at_plus_1d_rank").over(w_opened_at_to_date_agent_id), F.lit(0)), 4).alias("opened_at_plus_1d_rank_share_of_opened_at_to_dat"),
        F.round(F.avg("opened_at_plus_1d_rank").over(w_all_by_csat_per_requester_id), 2).alias("opened_at_plus_1d_rank_avg_2_prior"),
        F.rank().over(w_all_by_csat_per_requester_id_2).alias("csat_per_requester_id_rank_rank"),
    )
)

scored = (
    windowed
    .select(
        "id",
        "opened_at",
        "opened_at_plus_1d",
        "opened_at_plus_1d_rank",
        "opened_at_plus_1d_month_avg_csat_per_requester_i",
        "csat_per_requester_id_max_4",
        "csat_per_requester_id_rank",
        "tag",
        "opened_at_to_date_agent_id_sum_csat_per_requeste",
        "opened_at_plus_1d_rank_share_of_opened_at_to_dat",
        "opened_at_plus_1d_rank_avg_2_prior",
        "csat_per_requester_id_rank_rank",
        F.date_format(F.col("opened_at"), "yyyy-MM").alias("opened_at_label"),
    )
)

per_group = (
    scored
    .groupBy("tag")
    .agg(
        F.max("opened_at").alias("last_opened_at"),
        F.round(F.stddev("csat_per_requester_id_rank_rank"), 2).alias("stddev_csat_per_requester_id_rank_rank"),
    )
)

result = (
    per_group
    .filter(F.col("tag") == "bug")
    .select(
        "tag",
        "last_opened_at",
        F.substring(F.trim(F.col("tag")), 1, 4).alias("tag_prefix"),
        F.length(F.trim(F.col("tag"))).alias("tag_len"),
    )
    .orderBy(F.col("last_opened_at").asc_nulls_last())
)
