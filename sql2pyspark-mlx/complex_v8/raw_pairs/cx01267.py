from pyspark.sql import functions as F

recent_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

joined = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

filtered = (
    joined.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.artist_name").isNotNull()) & (F.col("a.genre") == "Classical"), "left_semi")
)

artist_id_stats = (
    filtered
    .groupBy("artist_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

compared = (
    filtered.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec") == F.col("g.max_duration_sec"))
    .select(
        "s.*",
        "g.max_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.max_duration_sec"), 2).alias("duration_sec_minus_max"),
    )
)

without_matches = (
    compared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec") > 213) & (F.array_contains(F.col("t.moods"), "party")), "left_anti")
)

artist_country_stats = (
    without_matches
    .groupBy("artist_country")
    .agg(F.avg("duration_sec_minus_max").alias("avg_duration_sec_minus_max"))
)

outliers = (
    without_matches.alias("s")
    .join(artist_country_stats.alias("g"), F.col("g.artist_country") == F.col("s.artist_country"), "inner")
    .filter(F.col("s.duration_sec_minus_max") > F.col("g.avg_duration_sec_minus_max"))
    .select(
        "s.*",
        "g.avg_duration_sec_minus_max",
        F.round(F.col("s.duration_sec_minus_max") - F.col("g.avg_duration_sec_minus_max"), 2).alias("duration_sec_minus_max_minus_avg"),
    )
)

result = (
    outliers
    .filter(F.col("avg_duration_sec_minus_max") >= -336.0)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "artist_country",
        "artist_debut_year",
        "max_duration_sec",
        "duration_sec_minus_max",
        "avg_duration_sec_minus_max",
        "duration_sec_minus_max_minus_avg",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.abs(F.col("duration_sec") - 352).alias("duration_sec_distance"),
    )
    .orderBy(F.col("duration_sec").desc_nulls_first())
)
