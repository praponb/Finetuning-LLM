from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")

recent_tracks = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-05-12").cast("date"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

scored = (
    recent_tracks
    .select(
        "*",
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.substring(F.trim(F.col("title_prefix")), 1, 4).alias("title_prefix_prefix"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played_2"),
    )
)

profiled = (
    scored.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.avg_play_ms_played", "p.avg_play_ms_played_2")
)

detail_rows = (
    profiled.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.moods",
        "s.title_prefix",
        "s.duration_sec_plus_bpm",
        "s.avg_play_ms_played",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

sequenced = (
    detail_rows
    .select(
        "id",
        "artist_id",
        "bpm",
        "moods",
        "title_prefix",
        "duration_sec_plus_bpm",
        "avg_play_ms_played",
        "play_id",
        "play_played_at",
        "play_listener_id",
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.round(F.avg("avg_play_ms_played").over(w_artist_id), 2).alias("artist_id_avg_avg_play_ms_played"),
    )
)

combined_details_artist = (
    sequenced.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.play_listener_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.title_prefix",
        "s.duration_sec_plus_bpm",
        "s.avg_play_ms_played",
        "s.play_id",
        "s.play_played_at",
        "s.play_listener_id",
        "s.bpm_vs_artist_id_avg",
        "s.artist_id_avg_avg_play_ms_played",
        F.col("a.genre").alias("artist_genre"),
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    combined_details_artist
    .select(
        "id",
        "bpm",
        "title_prefix",
        "duration_sec_plus_bpm",
        "avg_play_ms_played",
        "play_played_at",
        "bpm_vs_artist_id_avg",
        "artist_id_avg_avg_play_ms_played",
        "artist_genre",
    )
    .orderBy(F.col("bpm").desc())
)
