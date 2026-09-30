from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 5) | (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
        F.year(F.col("release_date")).alias("release_date_year"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

profiled = (
    scoped.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at")
)

annotated = (
    profiled
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "title_ref",
        "title_prefix",
        "first_played_at",
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref_2"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

result = (
    annotated
    .filter(F.col("first_played_at") < F.lit("2025-02-17 09:00:00").cast("timestamp"))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        "title_prefix",
        "first_played_at",
        "title_ref_2",
        "is_explicit_label",
    )
    .orderBy(F.col("title").desc(), F.col("first_played_at").desc(), "id")
    .limit(5)
)
