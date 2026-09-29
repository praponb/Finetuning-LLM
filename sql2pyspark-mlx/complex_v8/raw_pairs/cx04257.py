from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_total_ms_played_bu = Window.partitionBy("device").orderBy(F.col("total_ms_played_bucket").desc_nulls_first())
w_cnt_device = Window.partitionBy("cnt", "device")
w_device_by_max_ms_played_ties = Window.partitionBy("device").orderBy("max_ms_played")
w_device_cnt_by_total_play_is = Window.partitionBy("device", "cnt").orderBy(F.col("total_play_is_skipped_count").desc())

scoped = (
    df_plays
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    scoped.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

grouped = (
    with_counts
    .groupBy("device")
    .agg(
        F.count("*").alias("cnt"),
        F.max("ms_played").alias("max_ms_played"),
        F.sum("play_is_skipped_count").alias("total_play_is_skipped_count"),
        F.sum("ms_played_bucket").alias("total_ms_played_bucket"),
    )
)

qualified = (
    grouped
    .crossJoin(grouped.agg(F.avg("total_play_is_skipped_count").alias("overall_avg_total_play_is_skipped_count")))
    .filter(F.col("total_play_is_skipped_count") > F.col("overall_avg_total_play_is_skipped_count"))
    .drop("overall_avg_total_play_is_skipped_count")
)

with_activity = (
    qualified
    .crossJoin(qualified.agg(F.max("total_play_is_skipped_count").alias("overall_max_total_play_is_skipped_count")))
    .filter(F.col("total_play_is_skipped_count") < F.col("overall_max_total_play_is_skipped_count"))
    .drop("overall_max_total_play_is_skipped_count")
)

metrics = (
    with_activity
    .select(
        "device",
        F.round(F.cume_dist().over(w_device_by_total_ms_played_bu), 3).alias("total_ms_played_bucket_cume_dist"),
        F.max("total_ms_played_bucket").over(w_cnt_device).alias("cnt_device_max_total_ms_played_bucket"),
        F.dense_rank().over(w_device_by_max_ms_played_ties).alias("max_ms_played_dense_rank"),
        F.count(F.lit(1)).over(w_cnt_device).alias("rows_in_cnt_device"),
        F.round(F.cume_dist().over(w_device_cnt_by_total_play_is), 3).alias("total_play_is_skipped_count_cume_dist"),
    )
)

result = (
    metrics
    .groupBy("max_ms_played_dense_rank")
    .pivot("device", ["tv", "car", "phone"])
    .agg(F.max("total_ms_played_bucket_cume_dist").alias("max_v"), F.count("total_ms_played_bucket_cume_dist").alias("n"))
)
