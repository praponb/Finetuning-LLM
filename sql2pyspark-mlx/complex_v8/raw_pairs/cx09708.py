from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

with_details_artist = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") != "Pop"), "inner")
    .filter(~F.col("a.country").isin("JP", "TH"))
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.release_date_label",
        F.col("a.country").alias("artist_country"),
    )
)

enriched = (
    with_details_artist.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")), "inner")
    .filter(F.col("a.debut_year") < 2006)
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.release_date_label",
        "s.artist_country",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"))
)

profiled = (
    enriched.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.avg_play_ms_played")
)

enriched_2 = (
    profiled
    .filter(F.col("artist_id") <= 14)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("release_date_label")), " ", "_")).alias("release_date_label_slug"),
        F.coalesce(F.col("artist_debut_year"), F.lit(0)).alias("artist_debut_year_filled"),
        F.substring(F.trim(F.col("release_date_label")), 1, 4).alias("release_date_label_prefix"),
    )
)

with_activity = (
    enriched_2.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country").isNotNull()) & (F.col("a.is_verified")), "left_semi")
)

result = (
    with_activity
    .filter(F.col("duration_sec") <= 312)
    .select(
        "id",
        "artist_id",
        "artist_country",
        "avg_play_ms_played",
        "release_date_label_slug",
        "release_date_label_prefix",
    )
    .orderBy(F.col("avg_play_ms_played").desc(), F.col("id").asc_nulls_last())
    .limit(10)
)
