from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy("release_date", "id")

filtered = (
    df_tracks
    .filter(F.col("bpm") > 104)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
    )
)

unique_rows = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

without_matches = (
    unique_rows.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.country").isin("US", "KR")) & (F.col("a.genre") != "Indie"), "left_semi")
)

exploded_detail = (
    without_matches.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

filtered_2 = (
    exploded_detail.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

combined_details = (
    filtered_2.alias("s")
    .join(df_plays.alias("p"), (F.col("p.id") == F.col("s.play_id")) & (F.col("p.played_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp")), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("p.track_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.play_id",
        "s.play_source",
        "s.play_is_skipped",
        "s.play_ms_played",
        "s.play_played_at",
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

result = (
    combined_details
    .select("play_is_skipped")
    .distinct()
)
