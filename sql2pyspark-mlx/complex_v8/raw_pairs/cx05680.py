from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-04-21").cast("date"), F.lit("2025-11-29").cast("date"))) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
        F.greatest(F.col("bpm"), F.col("duration_sec")).alias("greatest_bpm_duration_sec"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("source") == "search", 1)).alias("play_source_count"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.min("played_at").alias("first_played_at"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        "p.avg_play_ms_played",
        "p.first_played_at",
    )
)

combined_details = (
    with_counts.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

combined_details_2 = (
    combined_details.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.least_bpm_duration_sec",
        "s.greatest_bpm_duration_sec",
        "s.release_date_label",
        "s.play_source_count",
        "s.avg_play_ms_played",
        "s.first_played_at",
        "s.artist_debut_year",
        "s.artist_genre",
        "s.artist_is_verified",
        "a.artist_name",
    )
)

qualified = (
    combined_details_2.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec").between(144, 409)), "left_semi")
    .filter(F.col("artist_debut_year") >= 1994)
)

per_label = (
    qualified
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "least_bpm_duration_sec",
        "greatest_bpm_duration_sec",
        "release_date_label",
        "play_source_count",
        "avg_play_ms_played",
        "first_played_at",
        "artist_debut_year",
        "artist_genre",
        "artist_is_verified",
        "artist_name",
        F.explode("moods").alias("mood"),
    )
)

result = (
    per_label
    .select(
        "id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "greatest_bpm_duration_sec",
        "release_date_label",
        "play_source_count",
        "avg_play_ms_played",
        "artist_genre",
        "mood",
    )
    .orderBy(
        "play_source_count",
        F.col("release_date").asc_nulls_last(),
        "id",
        "mood",
    )
    .limit(5)
)
