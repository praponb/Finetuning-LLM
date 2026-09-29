from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.array_contains(F.col("moods"), "workout")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        F.when(F.col("duration_sec") >= 344, "HIGH").when(F.col("duration_sec") >= 213, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

duration_sec_tier_stats = (
    base_tracks
    .groupBy("duration_sec_tier")
    .agg(F.max("moods_count").alias("max_moods_count"))
)

vs_group = (
    base_tracks.alias("s")
    .join(duration_sec_tier_stats.alias("g"), F.col("g.duration_sec_tier") == F.col("s.duration_sec_tier"), "inner")
    .filter(F.col("s.moods_count") == F.col("g.max_moods_count"))
    .select(
        "s.*",
        "g.max_moods_count",
        F.round(F.col("s.moods_count") - F.col("g.max_moods_count"), 2).alias("moods_count_minus_max"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.sum("duration_sec").alias("total_track_duration_sec"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
    )
)

with_stats = (
    vs_group.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.total_track_duration_sec",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
    )
)

annotated = (
    with_stats
    .select(
        "*",
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.size(F.col("moods")).alias("moods_count_2"),
    )
)

stats = (
    annotated
    .groupBy("duration_sec_tier", F.year("release_date").alias("release_date_year"))
    .agg(
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.sum(F.when(F.length(F.trim(F.col("title"))) >= 7, F.col("moods_count_minus_max")).otherwise(0)).alias("moods_count_minus_max_length_trim_title_7"),
        F.countDistinct("duration_sec_tier").alias("distinct_duration_sec_tiers"),
    )
)

result = (
    stats
    .select(
        "duration_sec_tier",
        "release_date_year",
        "title_list",
        "moods_count_minus_max_length_trim_title_7",
        "distinct_duration_sec_tiers",
    )
)
