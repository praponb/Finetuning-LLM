from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_played_at_runn = Window.partitionBy("is_skipped").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_listener_id = Window.partitionBy("device", "listener_id")
w_is_skipped_by_played_at_full = Window.partitionBy("is_skipped").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_played_at = Window.partitionBy("is_skipped").orderBy("played_at", "id")
w_track_id_device_by_second_ms = Window.partitionBy("track_id", "device").orderBy(F.col("second_ms_played").desc())
w_track_id_device_by_second_ms_2 = Window.partitionBy("track_id", "device").orderBy(F.col("second_ms_played").desc(), "id").rowsBetween(-6, Window.currentRow)
w_track_id_device_by_second_ms_3 = Window.partitionBy("track_id", "device").orderBy(F.col("second_ms_played").desc(), "id")

cleaned_plays = (
    df_plays
    .filter((F.col("source") == "album") & (F.hour(F.col("played_at")).between(9, 13)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

filtered = (
    cleaned_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")) & (F.col("p.track_id") <= 16), "left_semi")
)

sequenced = (
    filtered
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.min("ms_played").over(w_is_skipped_by_played_at_runn).alias("min_ms_played_so_far"),
        F.count(F.lit(1)).over(w_device_listener_id).alias("rows_in_device_listener_id"),
        F.nth_value("ms_played", 2).over(w_is_skipped_by_played_at_full).alias("second_ms_played"),
        F.sum("ms_played").over(w_device_listener_id).alias("device_listener_id_sum_ms_played"),
        F.lag("ms_played", 1, 0).over(w_is_skipped_by_played_at).alias("prev_ms_played"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source") != "search"), "left_anti")
)

metrics = (
    qualified
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "min_ms_played_so_far",
        "rows_in_device_listener_id",
        F.round(F.cume_dist().over(w_track_id_device_by_second_ms), 3).alias("second_ms_played_cume_dist"),
        F.round(F.avg("second_ms_played").over(w_track_id_device_by_second_ms_2), 2).alias("second_ms_played_avg_7"),
        F.ntile(5).over(w_track_id_device_by_second_ms_3).alias("second_ms_played_quintile"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

profiled = (
    metrics.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.min_play_ms_played",
    )
)

tagged = (
    profiled
    .filter(F.col("ms_played") > 52009)
    .withColumn("device_prefix", F.substring(F.trim(F.col("device")), 1, 4))
)

result = (
    tagged
    .filter(F.col("play_count") >= 2)
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "min_ms_played_so_far",
        "rows_in_device_listener_id",
        "second_ms_played_cume_dist",
        "second_ms_played_avg_7",
        "min_play_ms_played",
        "device_prefix",
        F.when(F.col("play_count") >= 6, "large").when(F.col("play_count") >= 2, "medium").otherwise("small").alias("play_count_tier"),
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        F.col("second_ms_played_avg_7").desc_nulls_first(),
    )
    .limit(20)
)
