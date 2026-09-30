from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
    )
)

artist_id_stats = (
    filtered_tracks
    .groupBy("artist_id")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

above_average = (
    filtered_tracks.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec") > F.col("g.avg_duration_sec"))
    .select(
        "s.*",
        "g.avg_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.avg_duration_sec"), 2).alias("duration_sec_minus_avg"),
    )
)

with_parent = (
    above_average.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.title_clean",
        "s.bpm_plus_duration_sec",
        "s.avg_duration_sec",
        "s.duration_sec_minus_avg",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

result = (
    with_parent
    .select("id", "duration_sec_minus_avg", "avg_duration_sec")
    .unpivot(
        ids=["id"],
        values=["duration_sec_minus_avg", "avg_duration_sec"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
)
