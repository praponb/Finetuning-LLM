from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.col("bpm") > 94) & (F.size(F.col("moods")) >= 1))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

joined_artist = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.moods",
        "s.title_prefix",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

with_activity = (
    joined_artist
    .crossJoin(joined_artist.agg(F.avg("duration_sec").alias("overall_avg_duration_sec")))
    .filter(F.col("duration_sec") > F.col("overall_avg_duration_sec"))
    .drop("overall_avg_duration_sec")
)

artist_id_stats = (
    with_activity
    .groupBy("artist_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

above_average = (
    with_activity.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec") == F.col("g.max_duration_sec"))
    .select(
        "s.*",
        "g.max_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.max_duration_sec"), 2).alias("duration_sec_minus_max"),
    )
)

tagged = (
    above_average
    .select(
        "*",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.length(F.trim(F.col("artist_genre"))).alias("artist_genre_len"),
        F.concat(F.col("title_prefix"), F.lit("#"), F.col("id").cast("string")).alias("title_prefix_ref"),
    )
)

result = (
    tagged
    .select(
        "id",
        "artist_id",
        "release_date",
        "moods",
        "title_prefix",
        "artist_genre",
        "max_duration_sec",
        "release_date_week",
        "artist_genre_len",
        F.coalesce(F.col("artist_genre"), F.lit("UNKNOWN")).alias("artist_genre_or_unknown"),
    )
    .orderBy("artist_genre", F.col("max_duration_sec").desc_nulls_first())
)
