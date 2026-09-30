from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_bucket = Window.orderBy(F.col("duration_sec_bucket").asc_nulls_last(), "id", "mood")
w_is_explicit_by_last_track_re = Window.partitionBy("is_explicit").orderBy(F.col("last_track_release_date").desc(), "id", "mood")
w_mood_artist_id = Window.partitionBy("mood", "artist_id")
w_mood_last_track_release_date = Window.partitionBy("mood", F.date_trunc("month", "last_track_release_date")).orderBy("duration_sec_bucket", "id", "mood").rowsBetween(-3, Window.currentRow)
w_duration_sec_bucket_rank = Window.partitionBy("duration_sec_bucket_rank")
w_duration_sec_bucket_seq_by_b = Window.partitionBy("duration_sec_bucket_seq").orderBy(F.col("bpm").desc(), "id", "mood").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_duration_sec_bucket_seq_by_b_2 = Window.partitionBy("duration_sec_bucket_seq").orderBy(F.col("bpm").desc(), "id", "mood").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_sec_bucket_seq_by_b_3 = Window.partitionBy("duration_sec_bucket_seq").orderBy(F.col("bpm").desc(), "id", "mood")
w_duration_sec_bucket_seq_by_b_4 = Window.partitionBy("duration_sec_bucket_seq").orderBy(F.col("bpm").desc())

base = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-11-14").cast("date"))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

flattened = (
    base
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "duration_sec_bucket",
        F.explode("moods").alias("mood"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.max("release_date").alias("last_track_release_date"))
)

with_stats = (
    flattened.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.last_track_release_date")
)

with_windows = (
    with_stats
    .select(
        "*",
        F.row_number().over(w_all_by_duration_sec_bucket).alias("duration_sec_bucket_seq"),
        F.rank().over(w_is_explicit_by_last_track_re).alias("last_track_release_date_rank"),
        F.sum("duration_sec_bucket").over(w_mood_last_track_release_date).alias("duration_sec_bucket_sum_4"),
        F.rank().over(w_all_by_duration_sec_bucket).alias("duration_sec_bucket_rank"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.max("last_track_release_date_rank").over(w_duration_sec_bucket_rank).alias("duration_sec_bucket_rank_max_last_track_release_"),
        F.nth_value("bpm", 2).over(w_duration_sec_bucket_seq_by_b).alias("second_bpm"),
        F.last("bpm", ignorenulls=True).over(w_duration_sec_bucket_seq_by_b_2).alias("last_known_bpm"),
        (F.col("duration_sec_bucket") - F.lag("duration_sec_bucket").over(w_duration_sec_bucket_seq_by_b_3)).alias("duration_sec_bucket_change"),
        F.round(F.col("duration_sec_bucket_rank") - F.avg("duration_sec_bucket_rank").over(w_duration_sec_bucket_rank), 2).alias("duration_sec_bucket_rank_vs_duration_sec_bucket_"),
        F.dense_rank().over(w_duration_sec_bucket_seq_by_b_4).alias("bpm_dense_rank"),
    )
)

with_children = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.duration_sec_bucket",
        "s.mood",
        "s.duration_sec_bucket_seq",
        "s.last_track_release_date_rank",
        "s.duration_sec_bucket_sum_4",
        "s.duration_sec_bucket_rank_vs_duration_sec_bucket_",
        "s.bpm_dense_rank",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

with_activity = (
    with_children.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.play_listener_id"), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "bpm",
        "duration_sec_bucket",
        "mood",
        "duration_sec_bucket_seq",
        "duration_sec_bucket_sum_4",
        "duration_sec_bucket_rank_vs_duration_sec_bucket_",
        "bpm_dense_rank",
        "play_id",
        "play_ms_played",
        "play_source",
        "play_played_at",
        F.abs(F.col("play_ms_played") - 218129).alias("play_ms_played_distance"),
        F.initcap(F.trim(F.col("play_source"))).alias("play_source_clean"),
    )
    .orderBy(
        F.col("bpm_dense_rank").asc(),
        F.col("duration_sec_bucket_sum_4").desc(),
    )
)
