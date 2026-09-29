from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("M%")) | (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

tagged = (
    filtered_tracks
    .select(
        "*",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
        F.concat_ws("|", F.col("title_prefix"), F.col("title")).alias("title_prefix_title_label"),
    )
)

combined_details_artist = (
    tagged.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.moods",
        "s.title_ref",
        "a.artist_name",
    )
)

scored = (
    combined_details_artist
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        "artist_name",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

without_matches = (
    scored.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") != "Pop") & (F.col("a.is_verified")), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
    )
    .orderBy(F.col("artist_id").asc_nulls_last(), F.col("id").asc())
)
