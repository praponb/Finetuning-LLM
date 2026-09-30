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
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

with_children_plays = (
    cleaned_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.bpm_filled",
        "s.is_explicit_label",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

enriched_artist = (
    with_children_plays.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played_2"),
    )
)

tagged = (
    enriched_artist
    .select(
        "*",
        F.coalesce(F.col("artist_country"), F.lit("UNKNOWN")).alias("artist_country_or_unknown"),
        F.concat(F.col("play_device"), F.lit("#"), F.col("play_ms_played").cast("string")).alias("play_device_ref"),
    )
)

with_details_artist = (
    tagged.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.moods",
        "s.bpm_filled",
        "s.play_id",
        "s.play_ms_played",
        "s.play_source",
        "s.artist_country",
        "s.artist_debut_year",
        "s.play_device",
        "s.play_ms_played_2",
        "s.artist_country_or_unknown",
        "s.play_device_ref",
        F.col("a.genre").alias("artist_genre"),
    )
)

enriched_artist_2 = (
    with_details_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.moods",
        "s.bpm_filled",
        "s.play_id",
        "s.play_ms_played",
        "s.play_source",
        "s.artist_country",
        "s.artist_debut_year",
        "s.artist_country_or_unknown",
        "s.artist_genre",
        F.col("a.country").alias("artist_country_2"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

joined_play = (
    enriched_artist_2.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.play_id",
        "s.play_ms_played",
        "s.play_source",
        "s.artist_country",
        "s.artist_genre",
        "s.artist_country_2",
        "s.artist_is_verified",
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

result = (
    joined_play
    .select(
        "id",
        "artist_id",
        "title",
        "play_ms_played",
        "play_source",
        "artist_country",
        "artist_country_2",
        "play_listener_id",
        F.coalesce(F.col("artist_genre"), F.lit("UNKNOWN")).alias("artist_genre_or_unknown"),
        (F.floor(F.col("play_ms_played") / 50000) * 50000).alias("play_ms_played_bucket"),
    )
    .orderBy(F.col("play_ms_played").desc())
)
