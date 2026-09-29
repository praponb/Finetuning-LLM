from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
    )
)

combined_details = (
    cleaned_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("t.title").alias("track_title"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_counts = (
    combined_details.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.last_played_at")
)

per_label = (
    with_counts
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "track_title",
        "track_bpm",
        "last_played_at",
        F.explode_outer("track_moods").alias("track_mood"),
    )
)

annotated = (
    per_label
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_played_at")).alias("days_since_last_played_at"),
    )
)

enriched = (
    annotated.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select("s.*", F.col("t.artist_id").alias("track_artist_id"))
)

result = (
    enriched
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "track_bpm",
        "last_played_at",
        "track_mood",
        "days_since_last_played_at",
        "track_artist_id",
        F.round(F.col("ms_played") / F.nullif(F.col("track_bpm"), F.lit(0)), 3).alias("ms_played_per_track_bpm"),
        F.when(F.col("track_mood").isin("chill", "party"), "group_a").when(F.col("track_mood") == "focus", "group_b").otherwise("other").alias("track_mood_group"),
    )
    .orderBy(F.col("track_artist_id").asc())
)
