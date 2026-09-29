from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-08-07").cast("date")) | (F.col("duration_sec") > 184))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
    )
)

enriched_artist = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.bpm",
        "s.title_len",
        "s.release_date_quarter",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

exploded_detail = (
    enriched_artist.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm",
        "s.release_date_quarter",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

with_parent_artist = (
    exploded_detail.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .filter(F.upper(F.trim(F.col("a.artist_name"))).like("L%"))
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
        F.col("p.source").alias("play_source_2"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    with_parent_artist
    .groupBy("is_explicit", "artist_country")
    .pivot("play_source", ["playlist", "radio", "album", "search"])
    .agg(F.sum("bpm"))
)
