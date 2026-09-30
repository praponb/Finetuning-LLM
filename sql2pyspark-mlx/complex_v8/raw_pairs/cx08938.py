from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter(F.col("bpm") >= 100)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

combined_details_artist = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.title_slug",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

combined_details = (
    combined_details_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre").isNotNull())
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.artist_debut_year",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("source") != "radio", 1)).alias("play_source_count"),
        F.count(F.when(F.col("source") != "album", 1)).alias("play_source_count_2"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    combined_details.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        F.coalesce(F.col("p.play_source_count_2"), F.lit(0)).alias("play_source_count_2"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

tagged = (
    summarized_rows
    .filter(F.col("release_date").isNotNull())
    .select(
        "*",
        F.length(F.trim(F.col("artist_genre"))).alias("artist_genre_len"),
        F.upper(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
    )
)

combined_details_2 = (
    tagged.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.artist_debut_year",
        "s.artist_name",
        "s.artist_genre",
        "s.artist_is_verified",
        "s.play_count",
        "s.artist_genre_len",
        "s.artist_name_clean",
        F.col("a.artist_name").alias("artist_artist_name"),
        F.col("a.is_verified").alias("artist_is_verified_2"),
        F.col("a.debut_year").alias("artist_debut_year_2"),
    )
)

with_details = (
    combined_details_2.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.artist_debut_year",
        "s.artist_name",
        "s.play_count",
        "s.artist_genre_len",
        "s.artist_artist_name",
        "s.artist_is_verified_2",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified_3"),
        F.col("a.genre").alias("artist_genre_2"),
    )
)

result = (
    with_details
    .select(
        "id",
        "artist_id",
        "release_date",
        "artist_debut_year",
        "artist_name",
        "play_count",
        "artist_genre_len",
        "artist_artist_name",
        "artist_is_verified_3",
        "artist_genre_2",
    )
    .orderBy(F.col("release_date").desc(), F.col("artist_genre_2").desc())
)
