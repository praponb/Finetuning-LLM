from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) | (F.col("bpm") <= 155))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        F.abs(F.col("duration_sec") - 183).alias("duration_sec_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

artist_id_stats = (
    prepared_tracks
    .groupBy("artist_id")
    .agg(F.avg("duration_sec_distance").alias("avg_duration_sec_distance"))
)

vs_group = (
    prepared_tracks.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec_distance") > F.col("g.avg_duration_sec_distance"))
    .select(
        "s.*",
        "g.avg_duration_sec_distance",
        F.round(F.col("s.duration_sec_distance") - F.col("g.avg_duration_sec_distance"), 2).alias("duration_sec_distance_minus_avg"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"), F.count("*").alias("play_count_2"))
)

with_counts = (
    vs_group.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

result = (
    with_counts
    .rollup("artist_id", "play_count", "play_count_2")
    .agg(
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.round(F.avg("play_count"), 2).alias("avg_play_count"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("play_count").asc_nulls_last(),
        F.col("play_count_2").asc_nulls_last(),
    )
)
