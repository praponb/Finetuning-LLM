from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter(F.length(F.trim(F.col("title"))) >= 6)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.array_contains(F.col("moods"), "focus").alias("has_focus"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
    )
)

with_counts = (
    cleaned.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.avg_play_ms_played",
    )
)

with_children = (
    with_counts.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.device").isin("speaker", "tv")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.title_clean",
        "s.has_focus",
        "s.play_count",
        "s.avg_play_ms_played",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

combined_details_artist = (
    with_children.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.play_count",
        "s.play_id",
        "s.play_is_skipped",
        "s.play_device",
        "s.play_listener_id",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    combined_details_artist
    .select(
        "id",
        "play_id",
        "play_count",
        "play_ms_played",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["play_count", "play_ms_played"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "play_id", "measure")
)
