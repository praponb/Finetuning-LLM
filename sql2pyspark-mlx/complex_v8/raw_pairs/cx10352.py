from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

grouped = (
    prepared
    .filter(F.col("title_len") > 4)
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("bpm") / F.nullif(F.sum("title_len"), F.lit(0)), 3).alias("bpm_to_title_len_ratio"),
        F.sum(F.when(F.col("is_explicit"), F.col("title_len")).otherwise(0)).alias("title_len_is_explicit_true"),
    )
)

screened = (
    grouped
    .crossJoin(grouped.agg(F.avg("title_len_is_explicit_true").alias("overall_avg_title_len_is_explicit_true")))
    .filter(F.col("title_len_is_explicit_true") > F.col("overall_avg_title_len_is_explicit_true"))
    .drop("overall_avg_title_len_is_explicit_true")
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("release_date").alias("first_track_release_date"),
        F.count("*").alias("track_count"),
    )
)

profiled = (
    screened.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.first_track_release_date",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

result = (
    profiled
    .select(
        "artist_id",
        F.col("title_len_is_explicit_true").alias("m_title_len_is_explicit_true"),
        F.col("cnt").alias("cnt_v"),
    )
    .unpivot(
        ids=["artist_id"],
        values=["m_title_len_is_explicit_true", "cnt_v"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("artist_id", "metric_name")
)
