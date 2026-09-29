from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at_full = Window.orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").desc())
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_all_by_closed_at_ties_2 = Window.orderBy("closed_at")
w_all_by_closed_at_full_2 = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_agent_id_by_closed_a = Window.partitionBy("channel", "agent_id").orderBy("closed_at")
w_all_by_closed_at_2 = Window.orderBy(F.col("closed_at").desc(), "id")
w_channel_closed_at_dense_rank = Window.partitionBy("channel", "closed_at_dense_rank")
w_all_by_second_closed_at = Window.orderBy(F.col("second_closed_at").desc(), "id")
w_closed_at_month_by_closed_at = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("closed_at_percent_rank_3").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_by_closed_at_seq = Window.partitionBy("channel").orderBy(F.col("closed_at_seq").asc_nulls_last(), "id")
w_closed_at_seq = Window.partitionBy("closed_at_seq")

base = (
    df_tickets
    .filter((F.month(F.col("closed_at")).isin(1, 2)) & (F.col("opened_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        F.date_format(F.col("opened_at"), "yyyy-MM").alias("opened_at_label"),
    )
)

with_trends = (
    base
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        F.last("closed_at").over(w_all_by_closed_at_full).alias("last_closed_at"),
        F.round(F.percent_rank().over(w_all_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
        F.row_number().over(w_all_by_closed_at).alias("closed_at_seq"),
        F.nth_value("closed_at", 2).over(w_all_by_closed_at_full).alias("second_closed_at"),
        F.round(F.percent_rank().over(w_all_by_closed_at_ties_2), 3).alias("closed_at_percent_rank_2"),
        F.nth_value("closed_at", 2).over(w_all_by_closed_at_full_2).alias("second_closed_at_3"),
        F.rank().over(w_channel_agent_id_by_closed_a).alias("closed_at_rank"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_closed_at_2))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.dense_rank().over(w_all_by_closed_at).alias("closed_at_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_closed_at), 3).alias("closed_at_percent_rank_3"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "closed_at",
        "channel",
        "tags",
        "closed_at_percent_rank",
        "closed_at_seq",
        "second_closed_at",
        "closed_at_percent_rank_2",
        "second_closed_at_3",
        "closed_at_rank",
        "minutes_since_prev_closed_at",
        "closed_at_dense_rank",
        "closed_at_percent_rank_3",
        F.count(F.lit(1)).over(w_channel_closed_at_dense_rank).alias("rows_in_channel_closed_at_dense_rank"),
        F.dense_rank().over(w_all_by_second_closed_at).alias("second_closed_at_dense_rank"),
        F.size(F.collect_set("agent_id").over(w_channel_closed_at_dense_rank)).alias("distinct_agent_id_in_channel_closed_at_dense_ran"),
        F.min("closed_at_rank").over(w_channel_closed_at_dense_rank).alias("channel_closed_at_dense_rank_min_closed_at_rank"),
        F.min("closed_at_dense_rank").over(w_channel_closed_at_dense_rank).alias("channel_closed_at_dense_rank_min_closed_at_dense"),
        F.nth_value("closed_at_percent_rank_2", 2).over(w_closed_at_month_by_closed_at).alias("second_closed_at_percent_rank_2"),
        (F.col("minutes_since_prev_closed_at") - F.lag("minutes_since_prev_closed_at").over(w_channel_by_closed_at_seq)).alias("minutes_since_prev_closed_at_change"),
        F.min("closed_at_percent_rank_3").over(w_closed_at_seq).alias("closed_at_seq_min_closed_at_percent_rank_3"),
    )
)

summary = (
    sequenced
    .groupBy(
        F.date_trunc("month", "second_closed_at").alias("second_closed_at_month"),
        "channel",
    )
    .agg(
        F.round(F.avg("second_closed_at_dense_rank"), 2).alias("avg_second_closed_at_dense_rank"),
        F.sum(F.when(F.col("channel") == "chat", 1).otherwise(0)).alias("n_channel_chat"),
        F.count("*").alias("row_count"),
        F.round(F.sum("closed_at_seq_min_closed_at_percent_rank_3"), 2).alias("total_closed_at_seq_min_closed_at_percent_rank_3"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
    )
)

result = (
    summary
    .select(
        "second_closed_at_month",
        "channel",
        "avg_second_closed_at_dense_rank",
        "n_channel_chat",
        "row_count",
        "total_closed_at_seq_min_closed_at_percent_rank_3",
        F.date_format(F.col("second_closed_at_month"), "yyyy-MM-dd HH:00").alias("second_closed_at_month_label"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
    )
    .orderBy(F.col("channel_slug").desc(), F.col("row_count").desc())
)
