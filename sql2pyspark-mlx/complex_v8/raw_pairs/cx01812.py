from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

scored = (
    recent_tracks
    .select(
        "*",
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
        F.month(F.col("release_date")).alias("release_date_month"),
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

artist_id_stats = (
    scored
    .groupBy("artist_id")
    .agg(F.max("release_date_month").alias("max_release_date_month"))
)

vs_group = (
    scored.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.release_date_month") == F.col("g.max_release_date_month"))
    .select(
        "s.*",
        "g.max_release_date_month",
        F.round(F.col("s.release_date_month") - F.col("g.max_release_date_month"), 2).alias("release_date_month_minus_max"),
    )
)

combined_details_artist = (
    vs_group.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre").isin("Pop", "Classical")), "inner")
    .filter(F.col("a.country") != "TH")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.moods",
        "s.release_date_month",
        "s.max_release_date_month",
        "s.release_date_month_minus_max",
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    combined_details_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "release_date_month",
        "max_release_date_month",
        "artist_country",
        F.coalesce(F.col("release_date_month"), F.lit(0)).alias("release_date_month_filled"),
    )
)
