from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

with_details = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") > 1994)
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.bpm",
        "s.moods_text",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    with_details
    .rollup("is_explicit", "artist_genre")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("artist_genre") == "HipHop", F.col("artist_debut_year")).otherwise(0)).alias("artist_debut_year_artist_genre_hiphop"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        "is_explicit",
        F.coalesce(F.col("artist_genre"), F.lit("ALL")).alias("artist_genre"),
        "gid",
        "artist_debut_year_artist_genre_hiphop",
        "n_rows",
        "distinct_ids",
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_genre").asc_nulls_last(),
    )
)
