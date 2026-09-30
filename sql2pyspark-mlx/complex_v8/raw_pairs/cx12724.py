from pyspark.sql import functions as F

base_plays = (
    df_plays
    .filter((F.month(F.col("played_at")).isin(1, 3)) & (F.col("source").isin("playlist", "album", "search")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

annotated = (
    base_plays
    .withColumn("source_ref", F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")))
)

with_details = (
    annotated.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_ref",
        F.col("t.title").alias("track_title"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.max("ms_played").alias("max_play_ms_played"))
)

with_counts = (
    with_details.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.max_play_ms_played")
)

grouped = (
    with_counts
    .filter(F.upper(F.trim(F.col("track_title"))).like("P%"))
    .groupBy("is_skipped")
    .agg(
        F.round(F.avg("max_play_ms_played"), 2).alias("avg_max_play_ms_played"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 3)
)

result = (
    grouped
    .rollup("cnt", "is_skipped")
    .agg(
        F.count("*").alias("cnt_2"),
        F.round(F.stddev("cnt"), 2).alias("stddev_cnt"),
    )
    .orderBy(F.col("cnt").asc_nulls_last(), F.col("is_skipped").asc_nulls_last())
)
