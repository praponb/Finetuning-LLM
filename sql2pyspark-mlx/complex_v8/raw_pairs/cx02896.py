from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_played_at_month_dayofwe = Window.partitionBy("play_played_at_month_dayofweek", "n_source_in_playlist_search")
w_play_played_at_month_dayofwe_2 = Window.partitionBy("play_played_at_month_dayofweek", "n_source_in_playlist_search").orderBy(F.col("play_played_at_month_dayofweek").desc(), "device", "play_played_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_plays = (
    df_plays
    .filter((F.col("device") == "phone") & (F.col("played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
        F.when(F.col("source").isin("search", "playlist"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
    )
)

exploded_detail_plays = (
    scoped_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") <= 16), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_prefix",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.device").alias("play_device"),
    )
)

source_stats = (
    exploded_detail_plays
    .groupBy("source")
    .agg(F.percentile_approx("ms_played", 0.5).alias("median_ms_played"))
)

compared = (
    exploded_detail_plays.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") >= F.col("g.median_ms_played"))
    .select(
        "s.*",
        "g.median_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.median_ms_played"), 2).alias("ms_played_minus_median"),
    )
)

grouped = (
    compared
    .groupBy(
        "device",
        F.date_trunc("month", "play_played_at").alias("play_played_at_month"),
    )
    .agg(
        F.min("ms_played").alias("min_ms_played"),
        F.sum(F.when(F.col("source").isin("playlist", "search"), 1).otherwise(0)).alias("n_source_in_playlist_search"),
        F.array_sort(F.collect_set("play_device")).alias("play_device_list"),
        F.sum("ms_played").alias("total_ms_played"),
        F.round(F.sum("ms_played") / F.nullif(F.sum("median_ms_played"), F.lit(0)), 3).alias("ms_played_to_median_ms_played_ratio"),
        F.max("ms_played_minus_median").alias("max_ms_played_minus_median_check"),
    )
    .filter(F.col("max_ms_played_minus_median_check") > -160000.0)
    .drop("max_ms_played_minus_median_check")
)

tagged = (
    grouped
    .select(
        "*",
        (F.floor(F.col("min_ms_played") / 50000) * 50000).alias("min_ms_played_bucket"),
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
        F.dayofweek(F.col("play_played_at_month")).alias("play_played_at_month_dayofweek"),
    )
)

with_activity = (
    tagged
    .crossJoin(tagged.agg(F.avg("min_ms_played").alias("overall_avg_min_ms_played")))
    .filter(F.col("min_ms_played") > F.col("overall_avg_min_ms_played"))
    .drop("overall_avg_min_ms_played")
)

with_windows = (
    with_activity
    .select(
        "*",
        F.count(F.lit(1)).over(w_play_played_at_month_dayofwe).alias("rows_in_play_played_at_month_dayofweek_n_source_"),
        F.first("n_source_in_playlist_search").over(w_play_played_at_month_dayofwe_2).alias("first_n_source_in_playlist_search"),
    )
)

result = (
    with_windows
    .select(
        "device",
        "play_played_at_month",
        "min_ms_played",
        "n_source_in_playlist_search",
        "total_ms_played",
        "ms_played_to_median_ms_played_ratio",
        "min_ms_played_bucket",
        "device_prefix",
        "play_played_at_month_dayofweek",
        "rows_in_play_played_at_month_dayofweek_n_source_",
        "first_n_source_in_playlist_search",
        F.coalesce(F.col("min_ms_played_bucket"), F.lit(0)).alias("min_ms_played_bucket_filled"),
    )
    .orderBy(
        F.col("play_played_at_month").desc(),
        "min_ms_played_bucket_filled",
        "device",
    )
    .limit(5)
)
