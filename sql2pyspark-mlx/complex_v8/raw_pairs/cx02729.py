from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_full = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("track_id", "listener_id").orderBy(F.col("is_skipped_avg_ms_played").desc(), F.col("id").desc())

recent = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_is_skipped), 2).alias("is_skipped_avg_ms_played"),
        F.sum("ms_played").over(w_is_skipped).alias("is_skipped_sum_ms_played"),
        F.last("ms_played", ignorenulls=True).over(w_all_by_ms_played_running).alias("last_known_ms_played"),
        F.nth_value("ms_played", 2).over(w_all_by_ms_played_full).alias("second_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_is_skipped), F.lit(0)), 4).alias("ms_played_share_of_is_skipped"),
    )
)

latest = (
    ranked
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

without_matches = (
    latest.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") >= 51883) & (F.col("p.played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("last_known_ms_played") >= 160264)
)

result = (
    without_matches
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "source_prefix",
        "device_prefix",
        "is_skipped_avg_ms_played",
        "second_ms_played",
        "ms_played_share_of_is_skipped",
        F.coalesce(F.col("second_ms_played"), F.lit(0)).alias("second_ms_played_filled"),
    )
    .orderBy(F.col("id").desc(), F.col("source_prefix").asc())
)
