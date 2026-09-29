from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at = Window.partitionBy("track_id").orderBy("played_at", "id")
w_listener_id_source = Window.partitionBy("listener_id", "source")
w_all_by_played_at_running = Window.orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_played_at_ties = Window.partitionBy("track_id").orderBy("played_at")
w_all_by_played_at = Window.orderBy(F.col("played_at").asc_nulls_last(), "id")
w_track_id_by_ms_played_moving = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, Window.currentRow)
w_listener_id_by_ms_played_sha = Window.partitionBy("listener_id").orderBy(F.col("ms_played_share_of_listener_id_source").desc(), "id").rowsBetween(-6, -1)
w_listener_id_by_ms_played_sha_2 = Window.partitionBy("listener_id").orderBy(F.col("ms_played_share_of_listener_id_source").desc(), "id")
w_all_by_played_at_2 = Window.orderBy("played_at", "id")
w_source = Window.partitionBy("source")

base_plays = df_plays

with_windows = (
    base_plays
    .select(
        "*",
        F.round(F.cume_dist().over(w_track_id_by_played_at), 3).alias("played_at_cume_dist"),
        F.size(F.collect_set("track_id").over(w_listener_id_source)).alias("distinct_track_id_in_listener_id_source"),
        F.first("ms_played", ignorenulls=True).over(w_all_by_played_at_running).alias("first_known_ms_played"),
        F.dense_rank().over(w_track_id_by_played_at).alias("played_at_dense_rank"),
        F.round(F.cume_dist().over(w_track_id_by_played_at_ties), 3).alias("played_at_cume_dist_2"),
        F.round(F.percent_rank().over(w_track_id_by_played_at), 3).alias("played_at_percent_rank"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id_source), F.lit(0)), 4).alias("ms_played_share_of_listener_id_source"),
        F.round(F.avg("ms_played").over(w_listener_id_source), 2).alias("listener_id_source_avg_ms_played"),
        F.row_number().over(w_all_by_played_at).alias("seq_no"),
        F.count(F.lit(1)).over(w_listener_id_source).alias("rows_in_listener_id_source"),
        F.max("ms_played").over(w_track_id_by_ms_played_moving).alias("ms_played_max_2"),
    )
)

top_rows = with_windows.filter(F.col("seq_no") == 1)

ranked = (
    top_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        "distinct_track_id_in_listener_id_source",
        "first_known_ms_played",
        "played_at_cume_dist_2",
        "seq_no",
        F.max("played_at_percent_rank").over(w_listener_id_by_ms_played_sha).alias("played_at_percent_rank_max_6_prior"),
        F.lead("first_known_ms_played").over(w_listener_id_by_ms_played_sha_2).alias("next_first_known_ms_played"),
        F.lead("rows_in_listener_id_source", 2).over(w_all_by_played_at_2).alias("next2_rows_in_listener_id_source"),
        F.sum("ms_played").over(w_source).alias("source_sum_ms_played"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
    )
)

rollup_base = (
    ranked
    .groupBy(F.date_format("played_at", "yyyy-MM").alias("played_at_ym"))
    .agg(
        F.sum(F.when(F.col("source") != "playlist", F.col("ms_played")).otherwise(0)).alias("ms_played_source_playlist"),
        F.percentile_approx("source_sum_ms_played", 0.5).alias("median_source_sum_ms_played"),
        F.sum("first_known_ms_played").alias("total_first_known_ms_played"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("cnt"),
    )
)

result = (
    rollup_base
    .select(
        "played_at_ym",
        "ms_played_source_playlist",
        "last_played_at",
        "cnt",
        F.date_format(F.col("last_played_at"), "yyyy-MM-dd").alias("last_played_at_label"),
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
    )
    .orderBy(F.col("cnt").desc())
)
