from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2025-04-02").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

scored = (
    recent
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "title_ref",
        "is_explicit_label",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

is_explicit_label_stats = (
    scored
    .groupBy("is_explicit_label")
    .agg(F.max("least_bpm_duration_sec").alias("max_least_bpm_duration_sec"))
)

outliers = (
    scored.alias("s")
    .join(is_explicit_label_stats.alias("g"), F.col("g.is_explicit_label") == F.col("s.is_explicit_label"), "inner")
    .filter(F.col("s.least_bpm_duration_sec") == F.col("g.max_least_bpm_duration_sec"))
    .select(
        "s.*",
        "g.max_least_bpm_duration_sec",
        F.round(F.col("s.least_bpm_duration_sec") - F.col("g.max_least_bpm_duration_sec"), 2).alias("least_bpm_duration_sec_minus_max"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count_2"),
    )
)

summarized_rows = (
    outliers.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

scored_2 = (
    summarized_rows
    .filter(F.col("max_least_bpm_duration_sec") <= 258.0)
    .select(
        "*",
        F.to_date(F.col("last_played_at")).alias("last_played_at_date"),
        F.round(F.col("play_count") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("play_count_per_duration_sec"),
    )
)

result = (
    scored_2
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "moods",
        "title_ref",
        "is_explicit_label",
        "least_bpm_duration_sec",
        "max_least_bpm_duration_sec",
        "least_bpm_duration_sec_minus_max",
        "play_count",
        "last_played_at_date",
        "play_count_per_duration_sec",
    )
)
