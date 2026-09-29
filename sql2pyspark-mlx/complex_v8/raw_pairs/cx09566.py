from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 4) & (F.size(F.col("moods")) >= 1))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        F.when(F.col("bpm") >= 142, "HIGH").when(F.col("bpm") >= 85, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
    )
)

enriched_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.moods",
        "s.release_date_quarter",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.count("*").alias("track_count"),
    )
)

summarized_rows = (
    enriched_artist.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

enriched = (
    summarized_rows
    .filter(F.lower(F.col("title")).like("%ni%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "artist_country",
        "artist_is_verified",
        "track_is_explicit_count",
        "track_count",
        F.round(F.col("track_count") / F.nullif(F.col("track_is_explicit_count"), F.lit(0)), 3).alias("track_count_per_track_is_explicit_count"),
    )
)

result = (
    enriched
    .filter(F.col("artist_id") <= 14)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "artist_country",
        "artist_is_verified",
        "track_is_explicit_count",
        F.lower(F.regexp_replace(F.trim(F.col("artist_country")), " ", "_")).alias("artist_country_slug"),
        (F.col("track_is_explicit_count") - F.col("track_count")).alias("track_is_explicit_count_minus_track_count"),
    )
    .orderBy(F.col("title").asc(), F.col("artist_id").desc_nulls_first(), "id")
    .limit(10)
)
