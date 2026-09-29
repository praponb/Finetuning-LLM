from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

with_details_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.genre").alias("artist_genre"), "a.artist_name")
)

result = (
    with_details_artist
    .cube("artist_genre", "artist_id")
    .agg(
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.lower(F.col("title")).like("%eo%"), F.col("bpm"))).alias("bpm_lower_title_like_eo"),
    )
    .orderBy(
        F.col("artist_genre").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
