from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.size(F.col("moods")) >= 1))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.round(F.avg("bpm"), 2).alias("avg_track_bpm"),
        F.min("release_date").alias("first_track_release_date"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.avg_track_bpm", "t.first_track_release_date")
)

with_children_plays = (
    with_counts.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.avg_track_bpm",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

with_details = (
    with_children_plays.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

artist_country_stats = (
    with_details
    .groupBy("artist_country")
    .agg(F.avg("play_ms_played").alias("avg_play_ms_played"))
)

above_average = (
    with_details.alias("s")
    .join(artist_country_stats.alias("g"), F.col("g.artist_country") == F.col("s.artist_country"), "inner")
    .filter(F.col("s.play_ms_played") > F.col("g.avg_play_ms_played"))
    .select(
        "s.*",
        "g.avg_play_ms_played",
        F.round(F.col("s.play_ms_played") - F.col("g.avg_play_ms_played"), 2).alias("play_ms_played_minus_avg"),
    )
)

derived = (
    above_average
    .filter(F.col("avg_play_ms_played") > 44500.0)
    .withColumn("play_source_len", F.length(F.trim(F.col("play_source"))))
)

result = (
    derived
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "avg_track_bpm",
        "play_id",
        "play_listener_id",
        "artist_country",
        "artist_debut_year",
        "avg_play_ms_played",
        "play_ms_played_minus_avg",
    )
    .orderBy(F.col("play_id").desc())
)
