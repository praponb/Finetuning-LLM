from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_title_len = Window.partitionBy("artist_id", "title_len")
w_is_explicit_flag_artist_id_b = Window.partitionBy("is_explicit_flag", "artist_id").orderBy("title_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_flag_artist_id_b_2 = Window.partitionBy("is_explicit_flag", "artist_id").orderBy("title_len")

recent = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "focus"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

sequenced = (
    recent
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "is_explicit_flag",
        F.round(F.col("title_len") / F.nullif(F.sum("title_len").over(w_artist_id_title_len), F.lit(0)), 4).alias("title_len_share_of_artist_id_title_len"),
        F.first("bpm", ignorenulls=True).over(w_is_explicit_flag_artist_id_b).alias("first_known_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_title_len), F.lit(0)), 4).alias("bpm_share_of_artist_id_title_len"),
        F.dense_rank().over(w_is_explicit_flag_artist_id_b_2).alias("title_len_dense_rank"),
        F.max("is_explicit_flag").over(w_is_explicit_flag_artist_id_b).alias("max_is_explicit_flag_so_far"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.sum("duration_sec").alias("total_track_duration_sec"))
)

with_stats = (
    sequenced.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.total_track_duration_sec")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

with_stats_2 = (
    with_stats.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

with_activity = (
    with_stats_2.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("t.title"))).like("N%")), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "title_len_share_of_artist_id_title_len",
        "first_known_bpm",
        "bpm_share_of_artist_id_title_len",
        "title_len_dense_rank",
        "play_count",
        F.coalesce(F.col("total_track_duration_sec"), F.lit(0)).alias("total_track_duration_sec_filled"),
        F.last_day(F.col("release_date")).alias("release_date_month_end"),
    )
)
