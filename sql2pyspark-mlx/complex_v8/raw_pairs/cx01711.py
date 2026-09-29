from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_source_len = Window.partitionBy("is_skipped", "source_len")
w_track_id_is_skipped_by_track = Window.partitionBy("track_id", "is_skipped").orderBy("track_duration_sec", "id")
w_is_skipped_by_track_duration = Window.partitionBy("is_skipped").orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_track_duration_2 = Window.partitionBy("is_skipped").orderBy("track_duration_sec", "id")

scoped = (
    df_plays
    .filter((F.col("ms_played") <= 218129) & (F.col("played_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix_2"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.source_prefix",
        "s.source_prefix_2",
        "s.source_len",
        F.col("t.title").alias("track_title"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_is_skipped_source_len), 2).alias("track_duration_sec_vs_is_skipped_source_len_avg"),
        (F.col("track_duration_sec") - F.lag("track_duration_sec").over(w_track_id_is_skipped_by_track)).alias("track_duration_sec_change"),
        F.ntile(5).over(w_track_id_is_skipped_by_track).alias("track_duration_sec_quintile"),
        F.nth_value("source_len", 2).over(w_is_skipped_by_track_duration).alias("second_source_len"),
        F.lag("track_duration_sec").over(w_is_skipped_by_track_duration_2).alias("prev_track_duration_sec"),
    )
)

source_stats = (
    metrics
    .groupBy("source")
    .agg(F.avg("track_duration_sec_quintile").alias("avg_track_duration_sec_quintile"))
)

above_average = (
    metrics.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.track_duration_sec_quintile") > F.col("g.avg_track_duration_sec_quintile"))
    .select(
        "s.*",
        "g.avg_track_duration_sec_quintile",
        F.round(F.col("s.track_duration_sec_quintile") - F.col("g.avg_track_duration_sec_quintile"), 2).alias("track_duration_sec_quintile_minus_avg"),
    )
)

result = (
    above_average
    .groupBy("track_id", "source_len")
    .pivot("source", ["radio", "search"])
    .agg(F.min("avg_track_duration_sec_quintile"))
    .orderBy("track_id", "source_len")
)
