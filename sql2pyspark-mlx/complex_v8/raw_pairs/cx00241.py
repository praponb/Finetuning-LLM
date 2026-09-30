from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.length(F.trim(F.col("title"))) >= 4))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
    )
)

combined_details_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") <= 2015)
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        "s.duration_sec_minus_bpm",
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

labeled = (
    combined_details_artist
    .filter(F.upper(F.trim(F.col("artist_name"))).like("K%"))
    .select(
        "*",
        F.abs(F.col("duration_sec") - 213).alias("duration_sec_distance"),
        F.substring(F.trim(F.col("artist_name")), 1, 2).alias("artist_name_prefix"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
    )
)

with_stats = (
    labeled.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at", "p.avg_play_ms_played")
)

result = (
    with_stats
    .cube("is_explicit", "artist_id")
    .agg(
        F.grouping("is_explicit").alias("is_total_is_explicit"),
        F.sum(F.when(~F.col("is_explicit"), F.col("duration_sec_distance"))).alias("duration_sec_distance_not_is_explicit"),
        F.count("*").alias("row_count"),
    )
)
