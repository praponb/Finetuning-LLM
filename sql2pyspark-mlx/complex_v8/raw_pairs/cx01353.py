from pyspark.sql import functions as F

recent = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.max("release_date").alias("last_track_release_date"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.last_track_release_date",
    )
)

with_children_plays = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.track_count",
        "s.last_track_release_date",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

derived = (
    with_children_plays
    .filter(F.col("play_listener_id") >= 18)
    .select(
        "id",
        "release_date",
        "track_count",
        "last_track_release_date",
        "play_id",
        "play_listener_id",
        "play_is_skipped",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

screened = (
    derived.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.play_listener_id")) & (~F.col("p.is_skipped")), "left_semi")
    .filter(F.col("track_count") <= 6)
)

line_level = (
    screened.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.is_skipped")), "inner")
    .select(
        "s.id",
        "s.release_date",
        "s.track_count",
        "s.last_track_release_date",
        "s.play_id",
        "s.play_listener_id",
        "s.title_len",
        F.col("p.id").alias("play_id_2"),
        F.col("p.listener_id").alias("play_listener_id_2"),
        F.col("p.is_skipped").alias("play_is_skipped_2"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    line_level
    .filter(F.col("title_len") >= 6)
    .select(
        "id",
        "release_date",
        "track_count",
        "last_track_release_date",
        "play_listener_id",
        "title_len",
        "play_id_2",
        "play_listener_id_2",
        "play_is_skipped_2",
        "play_ms_played",
        F.last_day(F.col("release_date")).alias("release_date_month_end"),
    )
)
