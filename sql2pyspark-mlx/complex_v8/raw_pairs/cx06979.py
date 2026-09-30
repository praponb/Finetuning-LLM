from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_listener_id = Window.partitionBy("listener_id")
w_listener_id_device = Window.partitionBy("listener_id", "device")
w_track_id_by_ms_played_ties = Window.partitionBy("track_id").orderBy(F.col("ms_played").asc_nulls_last())
w_device_played_at_month_by_da = Window.partitionBy("device", F.date_trunc("month", "played_at")).orderBy(F.col("days_since_played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_played_at_tie = Window.partitionBy("listener_id").orderBy("played_at")
w_device_played_at_month_by_da_2 = Window.partitionBy("device", F.date_trunc("month", "played_at")).orderBy(F.col("days_since_played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_ms_played_ful = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_played_at_month_by_da_3 = Window.partitionBy("device", F.date_trunc("month", "played_at")).orderBy(F.col("days_since_played_at").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_listener_id_by_played_at_tie_2 = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc())
w_all_by_listener_id_max_days = Window.orderBy("listener_id_max_days_since_played_at", "id")
w_played_at_dense_rank_listene = Window.partitionBy("played_at_dense_rank", "listener_id").orderBy("days_since_played_at_avg_1_prior", "id")
w_distinct_ids_distinct_listen = Window.partitionBy("distinct_ids", "distinct_listener_ids")

valid = (
    df_plays
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
        F.round(F.col("days_since_played_at") - F.avg("days_since_played_at").over(w_listener_id), 2).alias("days_since_played_at_vs_listener_id_avg"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
        F.round(F.avg("ms_played").over(w_listener_id_device), 2).alias("listener_id_device_avg_ms_played"),
        F.round(F.col("days_since_played_at") / F.nullif(F.sum("days_since_played_at").over(w_listener_id), F.lit(0)), 4).alias("days_since_played_at_share_of_listener_id"),
        F.round(F.percent_rank().over(w_track_id_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
        F.nth_value("ms_played", 2).over(w_device_played_at_month_by_da).alias("second_ms_played"),
        F.dense_rank().over(w_listener_id_by_played_at_tie).alias("played_at_dense_rank"),
        F.max("days_since_played_at").over(w_listener_id).alias("listener_id_max_days_since_played_at"),
        F.count(F.lit(1)).over(w_device_played_at_month_by_da_2).alias("running_count"),
        F.first("played_at").over(w_listener_id_by_ms_played_ful).alias("first_played_at"),
        F.nth_value("ms_played", 2).over(w_listener_id_by_ms_played_ful).alias("second_ms_played_2"),
        F.round(F.avg("days_since_played_at").over(w_device_played_at_month_by_da_3), 2).alias("days_since_played_at_avg_1_prior"),
        F.dense_rank().over(w_listener_id_by_played_at_tie_2).alias("played_at_dense_rank_2"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "days_since_played_at",
        "ms_played_vs_device_avg",
        "listener_id_device_avg_ms_played",
        "days_since_played_at_share_of_listener_id",
        "second_ms_played",
        "played_at_dense_rank",
        "listener_id_max_days_since_played_at",
        "first_played_at",
        "second_ms_played_2",
        "days_since_played_at_avg_1_prior",
        "played_at_dense_rank_2",
        F.min("running_count").over(w_listener_id).alias("listener_id_min_running_count"),
        F.row_number().over(w_all_by_listener_id_max_days).alias("seq_no"),
        (F.col("played_at_dense_rank_2") - F.lag("played_at_dense_rank_2").over(w_played_at_dense_rank_listene)).alias("played_at_dense_rank_2_change"),
    )
)

stats = (
    sequenced_2
    .filter(F.col("played_at_dense_rank").between(4, 9))
    .groupBy("played_at_dense_rank", "listener_id", "track_id")
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.round(F.stddev("second_ms_played"), 2).alias("stddev_second_ms_played"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("second_ms_played").alias("avg_second_ms_played_check"),
    )
    .filter(F.col("avg_second_ms_played_check") > 73625)
    .drop("avg_second_ms_played_check")
)

sequenced_3 = (
    stats
    .select(
        "played_at_dense_rank",
        "listener_id",
        "track_id",
        "stddev_second_ms_played",
        "distinct_ids",
        F.sum("distinct_ids").over(w_distinct_ids_distinct_listen).alias("distinct_ids_distinct_listener_ids_sum_distinct_"),
        F.max("distinct_ids").over(w_distinct_ids_distinct_listen).alias("distinct_ids_distinct_listener_ids_max_distinct_"),
    )
)

result = (
    sequenced_3
    .select(
        "played_at_dense_rank",
        "track_id",
        "stddev_second_ms_played",
        "distinct_ids",
        "distinct_ids_distinct_listener_ids_sum_distinct_",
        (F.col("distinct_ids") - F.col("played_at_dense_rank")).alias("distinct_ids_minus_played_at_dense_rank"),
        F.round(F.col("stddev_second_ms_played") * 1.1, 2).alias("stddev_second_ms_played_adj"),
    )
    .orderBy("stddev_second_ms_played")
)
