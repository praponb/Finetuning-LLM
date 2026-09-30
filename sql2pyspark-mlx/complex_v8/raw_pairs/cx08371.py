from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_is_skipped = Window.partitionBy("is_skipped")
w_listener_id_is_skipped_by_tr = Window.partitionBy("listener_id", "is_skipped").orderBy("track_bpm", "id")
w_track_id = Window.partitionBy("track_id")
w_listener_id_by_track_bpm = Window.partitionBy("listener_id").orderBy(F.col("track_bpm").desc(), "id")
w_track_id_by_track_bpm = Window.partitionBy("track_id").orderBy("track_bpm", "id")
w_listener_id_is_skipped_by_tr_2 = Window.partitionBy("listener_id", "is_skipped").orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_track_bpm_runnin = Window.partitionBy("track_id").orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_bpm_dense_rank_track_b = Window.partitionBy("track_bpm_dense_rank", "track_bpm_decile")
w_listener_id_by_last_track_bp = Window.partitionBy("listener_id").orderBy(F.col("last_track_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_next_track_bpm_full = Window.orderBy("next_track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_last_track_bp_2 = Window.partitionBy("listener_id").orderBy(F.col("last_track_bpm").desc())
w_all_by_next_track_bpm = Window.orderBy("next_track_bpm", "id")

cleaned = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(1, 2))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 225733, "large").when(F.col("ms_played") >= 151112, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

joined_track = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        F.col("t.title").alias("track_title"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.moods").alias("track_moods"),
    )
)

sequenced = (
    joined_track
    .select(
        "*",
        F.round(F.avg("track_bpm").over(w_track_id_listener_id), 2).alias("track_id_listener_id_avg_track_bpm"),
        F.min("track_bpm").over(w_is_skipped).alias("is_skipped_min_track_bpm"),
        F.ntile(10).over(w_listener_id_is_skipped_by_tr).alias("track_bpm_decile"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.dense_rank().over(w_listener_id_by_track_bpm).alias("track_bpm_dense_rank"),
        F.lead("track_bpm").over(w_track_id_by_track_bpm).alias("next_track_bpm"),
        F.last("track_bpm").over(w_listener_id_is_skipped_by_tr_2).alias("last_track_bpm"),
        F.max("track_bpm").over(w_track_id_by_track_bpm_runnin).alias("max_track_bpm_so_far"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_track_id_listener_id), F.lit(0)), 4).alias("track_bpm_share_of_track_id_listener_id"),
    )
)

top_rows = sequenced.filter(F.col("track_bpm_dense_rank") <= 2)

with_trends = (
    top_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "track_title",
        "track_bpm",
        "track_id_listener_id_avg_track_bpm",
        "track_bpm_decile",
        "rows_in_track_id",
        "next_track_bpm",
        "track_bpm_share_of_track_id_listener_id",
        F.size(F.collect_set("listener_id").over(w_track_bpm_dense_rank_track_b)).alias("distinct_listener_id_in_track_bpm_dense_rank_tra"),
        F.first("last_track_bpm").over(w_listener_id_by_last_track_bp).alias("first_last_track_bpm"),
        F.max("max_track_bpm_so_far").over(w_track_bpm_dense_rank_track_b).alias("track_bpm_dense_rank_track_bpm_decile_max_max_tr"),
        F.nth_value("rows_in_track_id", 2).over(w_all_by_next_track_bpm_full).alias("second_rows_in_track_id"),
        F.dense_rank().over(w_listener_id_by_last_track_bp_2).alias("last_track_bpm_dense_rank"),
        F.ntile(10).over(w_all_by_next_track_bpm).alias("next_track_bpm_decile"),
    )
)

result = (
    with_trends
    .filter(F.col("rows_in_track_id") <= 14)
    .select(
        "id",
        "track_title",
        "track_bpm",
        "track_id_listener_id_avg_track_bpm",
        "rows_in_track_id",
        "track_bpm_share_of_track_id_listener_id",
        "distinct_listener_id_in_track_bpm_dense_rank_tra",
        "first_last_track_bpm",
        "last_track_bpm_dense_rank",
        "next_track_bpm_decile",
    )
    .orderBy("next_track_bpm_decile")
)
