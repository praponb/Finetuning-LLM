from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_ms_played_ties = Window.partitionBy("device").orderBy("ms_played")
w_track_id_listener_id_by_ms_p = Window.partitionBy("track_id", "listener_id").orderBy("ms_played", "id")
w_device_by_ms_played = Window.partitionBy("device").orderBy("ms_played", "id")
w_is_skipped_device_by_ms_play = Window.partitionBy("is_skipped", "device").orderBy("ms_played", "id").rowsBetween(-6, Window.currentRow)
w_track_id_listener_id_by_ms_p_2 = Window.partitionBy("track_id", "listener_id").orderBy("ms_played")
w_is_skipped_device_by_ms_play_2 = Window.partitionBy("is_skipped", "device").orderBy("ms_played", "id")
w_listener_id = Window.partitionBy("listener_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_all_by_ms_played_prior_3 = Window.orderBy("ms_played", "id").rowsBetween(-3, -1)
w_device_track_id = Window.partitionBy("device", "track_id")
w_track_id = Window.partitionBy("track_id")

cleaned = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.round(F.percent_rank().over(w_device_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
        F.dense_rank().over(w_track_id_listener_id_by_ms_p).alias("ms_played_dense_rank"),
        F.dense_rank().over(w_device_by_ms_played).alias("ms_played_dense_rank_2"),
        F.sum("ms_played").over(w_is_skipped_device_by_ms_play).alias("ms_played_sum_7"),
        F.dense_rank().over(w_track_id_listener_id_by_ms_p_2).alias("ms_played_dense_rank_3"),
        F.rank().over(w_is_skipped_device_by_ms_play_2).alias("ms_played_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id), 2).alias("ms_played_vs_listener_id_avg"),
        F.ntile(5).over(w_all_by_ms_played).alias("ms_played_quintile"),
        F.max("ms_played").over(w_all_by_ms_played_prior_3).alias("ms_played_max_3_prior"),
        F.round(F.avg("ms_played").over(w_listener_id), 2).alias("listener_id_avg_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device_track_id), F.lit(0)), 4).alias("ms_played_share_of_device_track_id"),
        F.sum("ms_played").over(w_track_id).alias("track_id_sum_ms_played"),
    )
)

enriched = (
    metrics
    .filter(F.col("ms_played_sum_7") < 781750)
    .withColumn("device_prefix", F.substring(F.trim(F.col("device")), 1, 2))
)

result = (
    enriched
    .select(
        "id",
        "track_id",
        "ms_played",
        "is_skipped",
        "ms_played_percent_rank",
        "ms_played_dense_rank",
        "ms_played_dense_rank_3",
        "ms_played_vs_listener_id_avg",
        "ms_played_max_3_prior",
        "listener_id_avg_ms_played",
        "ms_played_share_of_device_track_id",
        "track_id_sum_ms_played",
        "device_prefix",
        F.greatest(F.col("track_id_sum_ms_played"), F.col("ms_played_dense_rank_2")).alias("greatest_track_id_sum_ms_played_ms_played_dense_"),
        F.concat_ws("-", F.col("device"), F.col("device_prefix")).alias("device_device_prefix_label"),
    )
    .orderBy(
        F.col("greatest_track_id_sum_ms_played_ms_played_dense_").desc(),
        F.col("device_device_prefix_label").asc_nulls_last(),
    )
)
