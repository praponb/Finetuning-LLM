from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_id = Window.partitionBy("csat", "agent_id")
w_channel_agent_id_by_requeste = Window.partitionBy("channel", "agent_id").orderBy("requester_id", "id")
w_all_by_csat = Window.orderBy(F.col("csat").desc_nulls_first(), "id")
w_all_by_csat_2 = Window.orderBy("csat", "id")
w_all_by_csat_ties = Window.orderBy("csat")
w_agent_id = Window.partitionBy("agent_id")
w_all_by_max_csat_percent_rank = Window.orderBy(F.col("max_csat_percent_rank").desc(), "agent_id").rowsBetween(-6, Window.currentRow)
w_distinct_channels_by_max_csa = Window.partitionBy("distinct_channels").orderBy(F.col("max_csat_percent_rank").asc_nulls_last(), "agent_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_tickets = (
    df_tickets
    .filter(F.col("agent_id") <= 8)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

sequenced = (
    cleaned_tickets
    .select(
        "*",
        F.count(F.lit(1)).over(w_csat_agent_id).alias("rows_in_csat_agent_id"),
        F.row_number().over(w_channel_agent_id_by_requeste).alias("requester_id_seq"),
        F.dense_rank().over(w_all_by_csat).alias("csat_dense_rank"),
        F.rank().over(w_all_by_csat).alias("csat_rank"),
        F.sum("csat").over(w_csat_agent_id).alias("csat_agent_id_sum_csat"),
        (F.col("csat") - F.lag("csat").over(w_channel_agent_id_by_requeste)).alias("csat_change"),
        F.dense_rank().over(w_channel_agent_id_by_requeste).alias("requester_id_dense_rank"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_csat_agent_id), F.lit(0)), 4).alias("requester_id_share_of_csat_agent_id"),
        F.round(F.avg("csat").over(w_csat_agent_id), 2).alias("csat_agent_id_avg_csat"),
        F.round(F.percent_rank().over(w_all_by_csat_2), 3).alias("csat_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_csat_ties), 3).alias("csat_percent_rank_2"),
    )
)

summary = (
    sequenced
    .groupBy("agent_id")
    .agg(
        F.max("csat_percent_rank").alias("max_csat_percent_rank"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
)

windowed = (
    summary
    .select(
        "*",
        F.round(F.col("distinct_channels") - F.avg("distinct_channels").over(w_agent_id), 2).alias("distinct_channels_vs_agent_id_avg"),
        F.round(F.avg("distinct_channels").over(w_all_by_max_csat_percent_rank), 2).alias("distinct_channels_avg_7"),
        F.nth_value("max_csat_percent_rank", 2).over(w_distinct_channels_by_max_csa).alias("second_max_csat_percent_rank"),
        F.max("max_csat_percent_rank").over(w_agent_id).alias("agent_id_max_max_csat_percent_rank"),
    )
)

result = (
    windowed
    .filter(F.col("second_max_csat_percent_rank") >= 0.3)
    .select(
        "agent_id",
        "max_csat_percent_rank",
        "distinct_channels",
        "distinct_channels_vs_agent_id_avg",
        "distinct_channels_avg_7",
        "second_max_csat_percent_rank",
        F.round(F.col("distinct_channels") / F.nullif(F.col("distinct_channels_avg_7"), F.lit(0)), 3).alias("distinct_channels_per_distinct_channels_avg_7"),
    )
    .orderBy(
        "max_csat_percent_rank",
        F.col("distinct_channels_vs_agent_id_avg").desc(),
        "agent_id",
    )
    .limit(10)
)
