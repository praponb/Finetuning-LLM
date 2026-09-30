from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_track_bpm_full = Window.orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id = Window.partitionBy("listener_id")
w_all_by_track_bpm = Window.orderBy("track_bpm", "id")
w_is_skipped_label_by_track_bp = Window.partitionBy("is_skipped_label").orderBy("track_bpm", "id")
w_all_by_played_at_ties = Window.orderBy(F.col("played_at").desc())
w_is_skipped_label_by_track_bp_2 = Window.partitionBy("is_skipped_label").orderBy("track_bpm")

filtered = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

joined_track = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.bpm") >= 85), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source_ref",
        "s.is_skipped_label",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

sequenced = (
    joined_track
    .select(
        "*",
        F.last("track_bpm").over(w_all_by_track_bpm_full).alias("last_track_bpm"),
        F.round(F.avg("ms_played").over(w_listener_id), 2).alias("listener_id_avg_ms_played"),
        F.ntile(3).over(w_all_by_track_bpm).alias("track_bpm_tercile"),
        F.sum("track_bpm").over(w_listener_id).alias("listener_id_sum_track_bpm"),
        F.first("played_at").over(w_all_by_track_bpm_full).alias("first_played_at"),
        F.ntile(3).over(w_is_skipped_label_by_track_bp).alias("track_bpm_tercile_2"),
        F.dense_rank().over(w_all_by_played_at_ties).alias("played_at_dense_rank"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_listener_id), F.lit(0)), 4).alias("track_bpm_share_of_listener_id"),
        F.round(F.cume_dist().over(w_is_skipped_label_by_track_bp_2), 3).alias("track_bpm_cume_dist"),
        F.round(F.avg("track_bpm").over(w_listener_id), 2).alias("listener_id_avg_track_bpm"),
        F.round(F.cume_dist().over(w_all_by_track_bpm), 3).alias("track_bpm_cume_dist_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source_ref",
        "is_skipped_label",
        "track_is_explicit",
        "last_track_bpm",
        "listener_id_avg_ms_played",
        "track_bpm_tercile",
        "listener_id_sum_track_bpm",
        "first_played_at",
        "track_bpm_tercile_2",
        "listener_id_sum_ms_played",
        "track_bpm_cume_dist",
        "listener_id_avg_track_bpm",
        "track_bpm_cume_dist_2",
        F.to_date(F.col("first_played_at")).alias("first_played_at_date"),
    )
    .orderBy(F.col("track_bpm_tercile_2").desc())
)
