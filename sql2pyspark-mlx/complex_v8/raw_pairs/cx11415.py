from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_source_by_total_play_ms_play = Window.partitionBy("source").orderBy(F.col("total_play_ms_played").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_by_total_play_ms_play_2 = Window.partitionBy("source").orderBy(F.col("total_play_ms_played").asc_nulls_last(), "id")
w_source_by_total_play_ms_play_3 = Window.partitionBy("source").orderBy(F.col("total_play_ms_played").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.avg_play_ms_played",
        "p.total_play_ms_played",
    )
)

with_details = (
    with_stats.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .filter(F.col("t.release_date").between(F.lit("2024-07-01").cast("date"), F.lit("2025-08-02").cast("date")))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.played_at_label",
        "s.avg_play_ms_played",
        "s.total_play_ms_played",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("a.country").alias("artist_country"),
    )
)

derived = (
    with_details
    .filter(F.col("track_release_date").between(F.lit("2024-05-12").cast("date"), F.lit("2025-08-02").cast("date")))
    .select(
        "*",
        F.concat(F.col("device"), F.lit("#"), F.col("track_artist_id").cast("string")).alias("device_ref"),
        F.concat_ws(" / ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

windowed = (
    derived
    .select(
        "*",
        F.round(F.col("total_play_ms_played") - F.avg("total_play_ms_played").over(w_device), 2).alias("total_play_ms_played_vs_device_avg"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.sum("avg_play_ms_played").over(w_source_by_total_play_ms_play).alias("running_avg_play_ms_played"),
        F.dense_rank().over(w_source_by_total_play_ms_play_2).alias("total_play_ms_played_dense_rank"),
        F.round(F.avg("avg_play_ms_played").over(w_source_by_total_play_ms_play_3), 2).alias("avg_play_ms_played_avg_4"),
        F.round(F.col("avg_play_ms_played") / F.nullif(F.sum("avg_play_ms_played").over(w_device), F.lit(0)), 4).alias("avg_play_ms_played_share_of_device"),
    )
)

result = (
    windowed
    .filter(F.col("running_avg_play_ms_played") >= 1203500.0)
    .select(
        "id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "played_at_label",
        "avg_play_ms_played",
        "track_release_date",
        "device_ref",
        "source_device_label",
        "total_play_ms_played_vs_device_avg",
        "rows_in_device",
        "running_avg_play_ms_played",
        "total_play_ms_played_dense_rank",
        "avg_play_ms_played_avg_4",
        "avg_play_ms_played_share_of_device",
    )
    .orderBy(
        F.col("source_device_label").desc(),
        F.col("avg_play_ms_played").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
