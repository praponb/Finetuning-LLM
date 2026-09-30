from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.size(F.col("moods")) >= 2))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

joined_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.country").isin("US", "JP")), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.title_prefix",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

without_matches = (
    joined_artist.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id") >= 13) & (~F.col("p.is_skipped")), "left_semi")
)

result = (
    without_matches
    .groupingSets(
        [["is_explicit", "artist_is_verified", "artist_id"], ["is_explicit"], []],
        "is_explicit", "artist_is_verified", "artist_id",
    )
    .agg(F.countDistinct("artist_id").alias("distinct_artist_ids"))
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_is_verified").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
