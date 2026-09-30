from pyspark.sql import functions as F

base = (
    df_tracks
    .filter(F.col("bpm") < 165)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("duration_sec") - 344).alias("duration_sec_distance"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    base.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

labeled = (
    with_counts
    .filter(F.col("is_explicit"))
    .withColumn("title_ref", F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")))
)

enriched = (
    labeled
    .filter(F.col("duration_sec") <= 365)
    .select(
        "*",
        (F.col("duration_sec_distance") - F.col("play_count")).alias("duration_sec_distance_minus_play_count"),
        F.greatest(F.col("max_play_ms_played"), F.col("duration_sec")).alias("greatest_max_play_ms_played_duration_sec"),
    )
)

result = (
    enriched
    .rollup("artist_id", "is_explicit", "play_count")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.sum(F.when(F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_true"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
        F.col("play_count").asc_nulls_last(),
    )
)
