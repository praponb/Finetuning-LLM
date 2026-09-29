from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.greatest(F.col("bpm"), F.col("duration_sec")).alias("greatest_bpm_duration_sec"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.abs(F.col("duration_sec") - 271).alias("duration_sec_distance"),
    )
)

filtered = (
    filtered_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") >= 89) & (F.array_contains(F.col("t.moods"), "party")), "left_anti")
)

without_matches = (
    filtered
    .crossJoin(filtered.agg(F.avg("bpm").alias("overall_avg_bpm")))
    .filter(F.col("bpm") > F.col("overall_avg_bpm"))
    .drop("overall_avg_bpm")
)

labeled = (
    without_matches
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        "greatest_bpm_duration_sec",
        "title_slug",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug_2"),
        F.round(F.col("duration_sec_distance") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_distance_per_bpm"),
        (F.floor(F.col("duration_sec_distance") / 100) * 100).alias("duration_sec_distance_bucket"),
    )
)

artist_id_stats = (
    labeled
    .groupBy("artist_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

compared = (
    labeled.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec") == F.col("g.max_duration_sec"))
    .select(
        "s.*",
        "g.max_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.max_duration_sec"), 2).alias("duration_sec_minus_max"),
    )
)

annotated = (
    compared
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        "title_slug_2",
        "duration_sec_distance_per_bpm",
        "duration_sec_distance_bucket",
        "max_duration_sec",
        "duration_sec_minus_max",
        F.concat(F.col("title_slug_2"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_slug_2_ref"),
    )
)

result = (
    annotated
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "moods",
        "duration_sec_distance_per_bpm",
        "duration_sec_distance_bucket",
        "max_duration_sec",
        "duration_sec_minus_max",
        "title_slug_2_ref",
    )
    .orderBy("duration_sec")
)
