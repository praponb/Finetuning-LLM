from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source")
w_all_by_total_ms_played_dista = Window.orderBy("total_ms_played_distance", "source")

recent_plays = (
    df_plays
    .filter(F.col("ms_played") <= 218129)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 236512, "A").when(F.col("ms_played") >= 120663, "B").otherwise("C").alias("ms_played_tier"),
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.abs(F.col("ms_played") - 183484).alias("ms_played_distance"),
    )
)

combined_details_track = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.source_prefix",
        "s.ms_played_distance",
        F.col("t.title").alias("track_title"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

per_group = (
    combined_details_track
    .groupBy("source")
    .agg(
        F.array_sort(F.collect_set("track_title")).alias("track_title_list"),
        F.max("ms_played_distance").alias("max_ms_played_distance"),
        F.sum("ms_played_distance").alias("total_ms_played_distance"),
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
    )
)

with_trends = (
    per_group
    .select(
        "*",
        F.sum("total_ms_played_distance").over(w_source).alias("source_sum_total_ms_played_distance"),
        F.round(F.cume_dist().over(w_all_by_total_ms_played_dista), 3).alias("total_ms_played_distance_cume_dist"),
        F.round(F.col("total_ms_played_distance") - F.avg("total_ms_played_distance").over(w_source), 2).alias("total_ms_played_distance_vs_source_avg"),
    )
)

result = (
    with_trends
    .select(
        "source",
        "max_ms_played_distance",
        "total_ms_played_distance",
        "stddev_ms_played",
        "source_sum_total_ms_played_distance",
        "total_ms_played_distance_cume_dist",
        "total_ms_played_distance_vs_source_avg",
    )
    .orderBy(
        F.col("source").desc(),
        F.col("total_ms_played_distance").desc_nulls_first(),
    )
)
