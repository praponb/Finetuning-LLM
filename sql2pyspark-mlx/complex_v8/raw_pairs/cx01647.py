from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_track_artist_id_by_pl = Window.partitionBy("source", "track_artist_id").orderBy("played_at", "id")
w_source_track_artist_id_by_ms = Window.partitionBy("source", "track_artist_id").orderBy(F.col("ms_played").desc(), "id")
w_listener_id = Window.partitionBy("listener_id")
w_source_track_artist_id_by_pl_2 = Window.partitionBy("source", "track_artist_id").orderBy("played_at")
w_source_by_ms_played_prior_3 = Window.partitionBy("source").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-3, -1)
w_source = Window.partitionBy("source")
w_source_by_ms_played = Window.partitionBy("source").orderBy("ms_played", "id")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_played_at_to_date_by_ms_play = Window.partitionBy(F.to_date("played_at")).orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_to_date_by_ms_play_2 = Window.partitionBy(F.to_date("played_at")).orderBy(F.col("ms_played").desc(), "id")
w_seq_no_ms_played_seq = Window.partitionBy("seq_no", "ms_played_seq")
w_played_at_rank_2_played_at_r = Window.partitionBy("played_at_rank_2", "played_at_rank").orderBy(F.col("seq_no").desc())
w_all_by_source_avg_ms_played = Window.orderBy(F.col("source_avg_ms_played").asc_nulls_last(), "id")
w_played_at_rank_2_played_at_r_2 = Window.partitionBy("played_at_rank_2", "played_at_rank").orderBy(F.col("seq_no").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_source_avg_ms_played_2 = Window.orderBy(F.col("source_avg_ms_played").asc_nulls_last())

base_plays = (
    df_plays
    .filter((F.col("source") != "radio") & (F.col("ms_played") > 56858))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
    )
)

enriched_track = (
    base_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

metrics = (
    enriched_track
    .select(
        "id",
        "listener_id",
        "ms_played",
        "source",
        "track_artist_id",
        F.rank().over(w_source_track_artist_id_by_pl).alias("played_at_rank"),
        F.row_number().over(w_source_track_artist_id_by_ms).alias("seq_no"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.round(F.percent_rank().over(w_source_track_artist_id_by_pl_2), 3).alias("played_at_percent_rank"),
        F.sum("ms_played").over(w_source_by_ms_played_prior_3).alias("ms_played_sum_3_prior"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2).alias("ms_played_vs_source_avg"),
        F.round(F.cume_dist().over(w_source_track_artist_id_by_pl_2), 3).alias("played_at_cume_dist"),
        F.size(F.collect_set("source").over(w_listener_id)).alias("distinct_source_in_listener_id"),
        F.ntile(3).over(w_source_by_ms_played).alias("ms_played_tercile"),
        F.round(F.avg("ms_played").over(w_source), 2).alias("source_avg_ms_played"),
        F.lag("played_at", 2).over(w_source_by_ms_played).alias("prev2_played_at"),
        F.rank().over(w_all_by_played_at).alias("played_at_rank_2"),
        F.nth_value("ms_played", 2).over(w_played_at_to_date_by_ms_play).alias("second_ms_played"),
        F.row_number().over(w_played_at_to_date_by_ms_play_2).alias("ms_played_seq"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "listener_id",
        "ms_played",
        "source",
        "track_artist_id",
        "seq_no",
        "rows_in_listener_id",
        "played_at_percent_rank",
        "ms_played_sum_3_prior",
        "played_at_cume_dist",
        "distinct_source_in_listener_id",
        "ms_played_tercile",
        "played_at_rank_2",
        "second_ms_played",
        F.round(F.col("played_at_rank_2") - F.avg("played_at_rank_2").over(w_seq_no_ms_played_seq), 2).alias("played_at_rank_2_vs_seq_no_ms_played_seq_avg"),
        F.max("ms_played_seq").over(w_seq_no_ms_played_seq).alias("seq_no_ms_played_seq_max_ms_played_seq"),
        F.rank().over(w_played_at_rank_2_played_at_r).alias("seq_no_rank"),
        F.lag("ms_played_sum_3_prior", 1, 0).over(w_all_by_source_avg_ms_played).alias("prev_ms_played_sum_3_prior"),
        F.count(F.lit(1)).over(w_seq_no_ms_played_seq).alias("rows_in_seq_no_ms_played_seq"),
        F.nth_value("ms_played_tercile", 2).over(w_played_at_rank_2_played_at_r_2).alias("second_ms_played_tercile"),
        F.dense_rank().over(w_all_by_source_avg_ms_played_2).alias("source_avg_ms_played_dense_rank"),
        F.nth_value("ms_played_vs_source_avg", 2).over(w_played_at_rank_2_played_at_r_2).alias("second_ms_played_vs_source_avg"),
    )
)

result = (
    windowed
    .select(
        "id",
        "listener_id",
        "ms_played",
        "track_artist_id",
        "rows_in_listener_id",
        "ms_played_sum_3_prior",
        "played_at_cume_dist",
        "distinct_source_in_listener_id",
        "ms_played_tercile",
        "played_at_rank_2",
        "second_ms_played",
        "seq_no_ms_played_seq_max_ms_played_seq",
        "seq_no_rank",
        "rows_in_seq_no_ms_played_seq",
        "second_ms_played_tercile",
        "source_avg_ms_played_dense_rank",
        "second_ms_played_vs_source_avg",
    )
    .orderBy("ms_played", F.col("track_artist_id").desc())
)
