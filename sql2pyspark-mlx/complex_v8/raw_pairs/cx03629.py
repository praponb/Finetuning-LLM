from pyspark.sql import functions as F

base = (
    df_tracks
    .filter(F.length(F.trim(F.col("title"))) >= 4)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

without_matches = (
    base.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

with_parent_artist = (
    without_matches.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    with_parent_artist
    .rollup("artist_id", "artist_country", "is_explicit")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("artist_country")).alias("artist_country_list"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
