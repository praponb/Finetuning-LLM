from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        F.array_contains(F.col("moods"), "party").alias("has_party"),
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

enriched_artist = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.title_clean",
        "s.has_party",
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"))
)

profiled = (
    enriched_artist.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.avg_play_ms_played")
)

with_parent = (
    profiled.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("a.artist_name"))).like("P%")), "inner")
    .filter(F.col("a.country").isNotNull())
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year_2"),
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

joined = (
    with_parent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.title_clean",
        "s.has_party",
        "s.artist_debut_year",
        "s.avg_play_ms_played",
        "s.artist_debut_year_2",
        "s.artist_is_verified",
        F.col("a.is_verified").alias("artist_is_verified_2"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("source") == "radio", 1)).alias("play_source_count"),
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    joined.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

combined_details_artist = (
    summarized_rows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.title_clean",
        "s.artist_debut_year",
        "s.avg_play_ms_played",
        "s.artist_debut_year_2",
        "s.play_source_count",
        "s.first_played_at",
        "s.play_count",
        F.col("a.is_verified").alias("artist_is_verified_3"),
        "a.artist_name",
    )
)

result = (
    combined_details_artist
    .filter(F.col("artist_debut_year_2") < 2015)
    .select(
        "id",
        "artist_id",
        "bpm",
        "title_clean",
        "avg_play_ms_played",
        "artist_debut_year_2",
        "play_source_count",
        "first_played_at",
        "play_count",
        "artist_name",
        (F.col("first_played_at") + F.expr("INTERVAL 7 DAYS")).alias("first_played_at_plus_7d"),
        F.month(F.col("first_played_at")).alias("first_played_at_month"),
    )
    .orderBy(F.col("bpm").desc(), "artist_debut_year_2", "id")
    .limit(5)
)
