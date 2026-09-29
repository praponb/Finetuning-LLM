from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm = Window.orderBy("bpm", "id")

recent = (
    df_tracks
    .filter((F.col("title").isNotNull()) & (F.array_contains(F.col("moods"), "workout")))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
    )
)

enriched_artist = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_windows = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "bpm",
        "artist_genre",
        (F.col("bpm") - F.lag("bpm").over(w_all_by_bpm)).alias("bpm_change"),
        F.lag("bpm", 1, 0).over(w_all_by_bpm).alias("prev_bpm"),
    )
)

annotated = (
    with_windows
    .filter(F.col("bpm_change") <= 90)
    .select(
        "*",
        F.round(F.col("bpm") / F.nullif(F.col("bpm_change"), F.lit(0)), 3).alias("bpm_per_bpm_change"),
        F.coalesce(F.col("prev_bpm"), F.lit(0)).alias("prev_bpm_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("artist_genre")), " ", "_")).alias("artist_genre_slug"),
    )
)

line_level = (
    annotated.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.artist_genre",
        "s.bpm_change",
        "s.prev_bpm_filled",
        "s.artist_genre_slug",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    line_level
    .groupBy("artist_id", "play_played_at")
    .pivot("artist_genre", ["Classical", "Pop", "HipHop", "Jazz"])
    .agg(F.count("prev_bpm_filled"))
    .orderBy("artist_id", "play_played_at")
)
