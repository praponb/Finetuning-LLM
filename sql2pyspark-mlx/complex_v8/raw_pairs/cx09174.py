from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_source_in_search_ra = Window.orderBy("n_source_in_search_radio")
w_play_device_count = Window.partitionBy("play_device_count")
w_all_by_play_count_running = Window.orderBy(F.col("play_count").desc(), "track_id", "source").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_plays
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 218129, "HIGH").when(F.col("ms_played") >= 168782, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.when(F.col("ms_played") >= 261579, "A").when(F.col("ms_played") >= 166517, "B").otherwise("C").alias("ms_played_tier_2"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_tier",
        "s.device_or_unknown",
        F.col("l.country").alias("listener_country"),
        F.col("l.interests").alias("listener_interests"),
    )
)

grouped = (
    with_parent
    .groupBy("track_id", "source")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("n_is_skipped_false"),
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
        F.round(100 * F.count(F.when(F.col("device") == "speaker", 1)) / F.count("*"), 1).alias("pct_device_speaker"),
        F.count(F.when(F.col("device") == "phone", 1)).alias("n_device_phone"),
        F.countDistinct("listener_country").alias("distinct_listener_countrys"),
        F.count(F.when(F.col("source").isin("search", "radio"), 1)).alias("n_source_in_search_radio"),
    )
)

screened = (
    grouped.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") > 160264) & (~F.col("p.device").isin("car", "desktop")), "left_semi")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count("*").alias("play_count_2"),
        F.count(F.when(F.col("device").isin("speaker", "tv", "phone"), 1)).alias("play_device_count"),
    )
)

with_counts = (
    screened.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

windowed = (
    with_counts
    .select(
        "*",
        F.dense_rank().over(w_all_by_n_source_in_search_ra).alias("n_source_in_search_radio_dense_rank"),
        F.round(F.col("n_is_skipped_true") - F.avg("n_is_skipped_true").over(w_play_device_count), 2).alias("n_is_skipped_true_vs_play_device_count_avg"),
        F.sum("distinct_listener_countrys").over(w_all_by_play_count_running).alias("running_distinct_listener_countrys"),
    )
)

result = (
    windowed
    .filter(F.col("play_count_2") > 3)
    .select(
        "track_id",
        "source",
        "n_is_skipped_false",
        "n_is_skipped_true",
        "pct_device_speaker",
        "n_source_in_search_radio",
        "play_count",
        "play_count_2",
        "n_source_in_search_radio_dense_rank",
        "n_is_skipped_true_vs_play_device_count_avg",
        "running_distinct_listener_countrys",
        F.coalesce(F.col("play_count"), F.lit(0)).alias("play_count_filled"),
    )
    .orderBy(F.col("play_count_filled").desc(), "source")
)
