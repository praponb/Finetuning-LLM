from pyspark.sql import functions as F

filtered = (
    df_artists
    .filter(F.length(F.trim(F.col("artist_name"))) >= 5)
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        F.lower(F.regexp_replace(F.trim(F.col("artist_name")), " ", "_")).alias("artist_name_slug"),
    )
)

exploded_detail_tracks = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.duration_sec") > 144), "left")
    .select(
        "s.id",
        "s.genre",
        "s.country",
        "s.artist_name_slug",
        F.col("t.id").alias("track_id"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

totals = (
    exploded_detail_tracks
    .groupBy("track_id", "genre", "country")
    .agg(
        F.array_sort(F.collect_set("country")).alias("country_list"),
        F.min("track_duration_sec").alias("min_track_duration_sec"),
        F.array_sort(F.collect_set("genre")).alias("genre_list"),
        F.avg("track_duration_sec").alias("avg_track_duration_sec_check"),
    )
    .filter(F.col("avg_track_duration_sec_check") > 183)
    .drop("avg_track_duration_sec_check")
)

result = (
    totals
    .filter(F.col("track_id").isNotNull())
    .select(
        "track_id",
        "country",
        "country_list",
        "min_track_duration_sec",
        "genre_list",
        F.substring(F.trim(F.col("genre")), 1, 4).alias("genre_prefix"),
        F.array_join(F.array_sort(F.col("country_list")), ",").alias("country_list_text"),
    )
    .orderBy(F.col("country_list_text").desc())
)
