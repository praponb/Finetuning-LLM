from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.max("duration_sec").alias("max_track_duration_sec"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count_2"),
    )
)

with_stats = (
    cleaned_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        "t.max_track_duration_sec",
        F.coalesce(F.col("t.track_is_explicit_count_2"), F.lit(0)).alias("track_is_explicit_count_2"),
    )
)

filtered = (
    with_stats
    .crossJoin(with_stats.agg(F.max("title_len").alias("overall_max_title_len")))
    .filter(F.col("title_len") < F.col("overall_max_title_len"))
    .drop("overall_max_title_len")
)

stats = (
    filtered
    .groupBy(
        F.when(F.col("max_track_duration_sec") >= 304, "A").when(F.col("max_track_duration_sec") >= 238, "B").otherwise("C").alias("max_track_duration_sec_tier"),
        "artist_id",
    )
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    stats
    .select("max_track_duration_sec_tier", "artist_id", "distinct_ids")
    .orderBy(F.col("artist_id").desc(), F.col("distinct_ids").desc())
)
