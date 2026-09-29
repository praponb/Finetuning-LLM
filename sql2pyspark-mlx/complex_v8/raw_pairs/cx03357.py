from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.array_contains(F.col("moods"), "focus").alias("has_focus"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

with_activity = (
    cleaned_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.played_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("artist_id").isNotNull())
)

enriched = (
    with_activity.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.has_focus",
        "s.duration_sec_per_bpm",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

per_label = (
    enriched
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "has_focus",
        "duration_sec_per_bpm",
        "artist_name",
        "artist_genre",
        "artist_debut_year",
        F.explode("moods").alias("mood"),
    )
)

result = (
    per_label
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "has_focus",
        "artist_name",
        "artist_genre",
        "artist_debut_year",
        "mood",
        F.length(F.trim(F.col("artist_name"))).alias("artist_name_len"),
    )
)
