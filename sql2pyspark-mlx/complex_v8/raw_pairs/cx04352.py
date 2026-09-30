from pyspark.sql import functions as F

recent = (
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
        F.when(F.col("duration_sec") >= 392, "HIGH").when(F.col("duration_sec") >= 164, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.min("release_date").alias("first_track_release_date"),
        F.min("release_date").alias("first_track_release_date_2"),
    )
)

profiled = (
    recent.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.first_track_release_date",
        "t.first_track_release_date_2",
    )
)

detail_rows_plays = (
    profiled.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.duration_sec_tier",
        "s.release_date_plus_7d",
        "s.track_count",
        "s.first_track_release_date",
        "s.first_track_release_date_2",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

derived = (
    detail_rows_plays
    .filter(~F.col("play_source").isin("album", "playlist"))
    .select(
        "*",
        F.date_trunc("quarter", F.col("release_date_plus_7d")).alias("release_date_plus_7d_quarter"),
    )
)

enriched_artist = (
    derived.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") <= 2006)
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    enriched_artist
    .filter(F.col("play_source") != "playlist")
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_tier",
        "track_count",
        "first_track_release_date",
        "first_track_release_date_2",
        "play_id",
        "play_device",
        "play_is_skipped",
        "release_date_plus_7d_quarter",
        "artist_country",
        "artist_debut_year",
        F.greatest(F.col("artist_debut_year"), F.col("track_count")).alias("greatest_artist_debut_year_track_count"),
        F.datediff(F.col("release_date_plus_7d"), F.col("first_track_release_date")).alias("days_first_track_release_date_to_release_date_pl"),
    )
    .orderBy(F.col("artist_country").asc(), "release_date_plus_7d_quarter")
)
