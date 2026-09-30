from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source")
w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_ties = Window.orderBy(F.col("ms_played").desc_nulls_first())
w_latest = Window.partitionBy("source", "track_id").orderBy(F.col("track_duration_sec").desc(), F.col("id").desc())

prepared_plays = (
    df_plays
    .filter(F.col("listener_id") >= 4)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
    )
)

metrics = (
    prepared_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2).alias("ms_played_vs_source_avg"),
        F.count(F.lit(1)).over(w_all_by_ms_played_running).alias("running_count"),
        F.dense_rank().over(w_all_by_ms_played_ties).alias("ms_played_dense_rank"),
        F.max("ms_played").over(w_source).alias("source_max_ms_played"),
    )
)

enriched = (
    metrics.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("l.birth_year").between(1966, 1981))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.source",
        "s.source_max_ms_played",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("t.moods").alias("track_moods"),
    )
)

combined_details_track = (
    enriched.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.source",
        "s.source_max_ms_played",
        "s.track_moods",
        F.col("t.moods").alias("track_moods_2"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

latest_per_key = (
    combined_details_track
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

profiled = (
    latest_per_key.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

result = (
    profiled
    .filter(F.col("play_count") >= 2)
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "track_moods",
        "track_moods_2",
        "track_artist_id",
        "track_duration_sec",
    )
    .orderBy("listener_id")
)
