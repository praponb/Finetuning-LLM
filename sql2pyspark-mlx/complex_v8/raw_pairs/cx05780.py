from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_play_count = Window.orderBy("play_count", "id")
w_all_by_play_count_running = Window.orderBy("play_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_last_played_at_mont = Window.partitionBy("track_id", F.month("last_played_at"))

filtered = (
    df_plays
    .filter((F.col("device") == "desktop") & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.avg_play_ms_played",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

with_trends = (
    with_counts
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device_ref",
        "ms_played_filled",
        "last_played_at",
        "play_count",
        F.ntile(4).over(w_all_by_play_count).alias("play_count_quartile"),
        (F.col("play_count") - F.lag("play_count").over(w_all_by_play_count)).alias("play_count_change"),
        F.sum("ms_played").over(w_all_by_play_count_running).alias("running_ms_played"),
        F.size(F.collect_set("listener_id").over(w_track_id_last_played_at_mont)).alias("distinct_listener_id_in_track_id_last_played_at_"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_play_count)).alias("ms_played_change"),
    )
)

labeled = (
    with_trends
    .select(
        "id",
        "track_id",
        "ms_played",
        "last_played_at",
        "play_count",
        "play_count_quartile",
        "play_count_change",
        "running_ms_played",
        "distinct_listener_id_in_track_id_last_played_at_",
        "ms_played_change",
        F.concat(F.col("device_ref"), F.lit("#"), F.col("play_count_quartile").cast("string")).alias("device_ref_ref"),
        F.coalesce(F.col("play_count_change"), F.lit(0)).alias("play_count_change_filled"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count_2"),
        F.min("played_at").alias("first_played_at"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

with_counts_2 = (
    labeled.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
        "p.first_played_at",
        "p.min_play_ms_played",
    )
)

joined_track = (
    with_counts_2.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.release_date").isNotNull())
    .select(
        "s.id",
        "s.track_id",
        "s.ms_played",
        "s.last_played_at",
        "s.play_count",
        "s.play_count_change",
        "s.running_ms_played",
        "s.distinct_listener_id_in_track_id_last_played_at_",
        "s.ms_played_change",
        "s.play_count_change_filled",
        "s.play_count_2",
        "s.first_played_at",
        "s.min_play_ms_played",
        F.col("t.release_date").alias("track_release_date"),
    )
)

derived = (
    joined_track
    .select(
        "*",
        F.add_months(F.col("track_release_date"), 3).alias("track_release_date_plus_3m"),
        F.last_day(F.col("track_release_date")).alias("track_release_date_month_end"),
    )
)

result = (
    derived
    .select(
        "id",
        "track_id",
        "ms_played",
        "last_played_at",
        "play_count",
        "play_count_change",
        "running_ms_played",
        "distinct_listener_id_in_track_id_last_played_at_",
        "ms_played_change",
        "play_count_change_filled",
        "play_count_2",
        "min_play_ms_played",
        "track_release_date",
        "track_release_date_plus_3m",
        "track_release_date_month_end",
    )
    .orderBy(F.col("track_release_date").desc())
)
