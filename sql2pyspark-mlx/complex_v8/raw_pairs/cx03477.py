from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_listener_id_by_played = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_source_listener_id_by_played_2 = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_listener_id_by_played_3 = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_source_listener_id_by_played_4 = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_source_len_ties = Window.orderBy(F.col("source_len").asc_nulls_last())
w_source_listener_id_by_played_5 = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_source_listener_id_by_played_6 = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_track_id_source_by_source_le = Window.partitionBy("track_id", "source").orderBy(F.col("source_len").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_source_by_source_le_2 = Window.partitionBy("track_id", "source").orderBy(F.col("source_len").desc_nulls_first(), "id")
w_all_by_source_len = Window.orderBy(F.col("source_len").desc(), "id")
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_all_by_source_len_full = Window.orderBy(F.col("source_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_source_len_running = Window.orderBy(F.col("source_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_played_at_run = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_len_dense_rank_source = Window.partitionBy("source_len_dense_rank", "source_len_rank").orderBy(F.col("second_source_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_second_source_len = Window.partitionBy("rn", "second_source_len")
w_source_len_dense_rank_source_2 = Window.partitionBy("source_len_dense_rank", "source_len_rank").orderBy(F.col("second_source_len").desc(), "id")
w_source_len_rank = Window.partitionBy("source_len_rank")
w_source_len_dense_rank_source_3 = Window.partitionBy("source_len_dense_rank", "source_len_rank").orderBy(F.col("second_source_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn_by_source_len_avg_2 = Window.partitionBy("rn").orderBy("source_len_avg_2", "id")

scoped_plays = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("device").isin("phone", "speaker")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

ranked = (
    scoped_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        F.round(F.avg("source_len").over(w_source_listener_id_by_played), 2).alias("source_len_avg_1_prior"),
        F.nth_value("source_len", 2).over(w_source_listener_id_by_played_2).alias("second_source_len"),
        F.sum("source_len").over(w_source_listener_id_by_played_3).alias("source_len_sum_2_prior"),
        F.round(F.avg("source_len").over(w_source_listener_id_by_played_4), 2).alias("source_len_avg_2"),
        F.rank().over(w_all_by_source_len_ties).alias("source_len_rank"),
        F.round(F.avg("source_len").over(w_source_listener_id_by_played_5), 2).alias("source_len_avg_4"),
        (F.col("source_len") - F.lag("source_len").over(w_source_listener_id_by_played_6)).alias("source_len_change"),
        F.last("played_at").over(w_track_id_source_by_source_le).alias("last_played_at"),
        F.round(F.cume_dist().over(w_track_id_source_by_source_le_2), 3).alias("source_len_cume_dist"),
        F.dense_rank().over(w_all_by_source_len).alias("source_len_dense_rank"),
        F.row_number().over(w_listener_id_by_played_at).alias("rn"),
        F.last("played_at").over(w_all_by_source_len_full).alias("last_played_at_2"),
        F.min("source_len").over(w_all_by_source_len_running).alias("min_source_len_so_far"),
        F.sum("source_len").over(w_listener_id_by_played_at_run).alias("running_source_len"),
        F.ntile(3).over(w_all_by_source_len).alias("source_len_tercile"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.first("source_len_tercile", ignorenulls=True).over(w_source_len_dense_rank_source).alias("first_known_source_len_tercile"),
        F.size(F.collect_set("source").over(w_rn_second_source_len)).alias("distinct_source_in_rn_second_source_len"),
        F.round(F.percent_rank().over(w_source_len_dense_rank_source_2), 3).alias("second_source_len_percent_rank"),
        F.round(F.col("second_source_len") - F.avg("second_source_len").over(w_source_len_rank), 2).alias("second_source_len_vs_source_len_rank_avg"),
        F.round(F.col("min_source_len_so_far") / F.nullif(F.sum("min_source_len_so_far").over(w_rn_second_source_len), F.lit(0)), 4).alias("min_source_len_so_far_share_of_rn_second_source_"),
        F.nth_value("min_source_len_so_far", 2).over(w_source_len_dense_rank_source_3).alias("second_min_source_len_so_far"),
        F.ntile(3).over(w_rn_by_source_len_avg_2).alias("source_len_avg_2_tercile"),
        F.size(F.collect_set("listener_id").over(w_rn_second_source_len)).alias("distinct_listener_id_in_rn_second_source_len"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "listener_id",
        "track_id",
        "source_len_avg_1_prior",
        "second_source_len",
        "source_len_sum_2_prior",
        "source_len_avg_2",
        "source_len_avg_4",
        "last_played_at",
        "source_len_cume_dist",
        "source_len_dense_rank",
        "rn",
        "last_played_at_2",
        "min_source_len_so_far",
        "running_source_len",
        "source_len_tercile",
        "second_source_len_percent_rank",
        "second_source_len_vs_source_len_rank_avg",
        "min_source_len_so_far_share_of_rn_second_source_",
        "source_len_avg_2_tercile",
        "distinct_listener_id_in_rn_second_source_len",
        F.dayofweek(F.col("last_played_at")).alias("last_played_at_dayofweek"),
        F.when(F.col("source_len_avg_2") >= 11.0, "HIGH").when(F.col("source_len_avg_2") >= 5.0, "MEDIUM").otherwise("LOW").alias("source_len_avg_2_tier"),
    )
)
