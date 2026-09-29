from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_device_len_by_ms_play = Window.partitionBy("source", "device_len").orderBy("ms_played_bucket", "id")
w_source_device_len_by_ms_play_2 = Window.partitionBy("source", "device_len").orderBy("ms_played_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_listener_id_by_ms_p = Window.partitionBy("track_id", "listener_id").orderBy(F.col("ms_played_bucket").desc())
w_source_device_len_by_ms_play_3 = Window.partitionBy("source", "device_len").orderBy("ms_played_bucket")
w_device_len_device_by_ms_play = Window.partitionBy("device_len", "device").orderBy(F.col("ms_played_bucket").desc(), "id")
w_track_id_listener_id_by_ms_p_2 = Window.partitionBy("track_id", "listener_id").orderBy(F.col("ms_played_bucket").desc(), "id")
w_listener_id = Window.partitionBy("listener_id")
w_device_len_device_by_ms_play_2 = Window.partitionBy("device_len", "device").orderBy(F.col("ms_played_bucket").desc())
w_source_device_len_by_ms_play_4 = Window.partitionBy("source", "device_len").orderBy("ms_played_bucket", "id").rowsBetween(-1, Window.currentRow)

cleaned_plays = (
    df_plays
    .filter((F.col("track_id") >= 19) & (F.col("device").isin("speaker", "desktop")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

with_windows = (
    cleaned_plays
    .select(
        "*",
        (F.col("ms_played_bucket") - F.lag("ms_played_bucket").over(w_source_device_len_by_ms_play)).alias("ms_played_bucket_change"),
        F.sum("ms_played_bucket").over(w_source_device_len_by_ms_play_2).alias("running_ms_played_bucket"),
        F.rank().over(w_track_id_listener_id_by_ms_p).alias("ms_played_bucket_rank"),
        F.dense_rank().over(w_source_device_len_by_ms_play_3).alias("ms_played_bucket_dense_rank"),
        F.row_number().over(w_device_len_device_by_ms_play).alias("rn"),
        (F.col("device_len") - F.lag("device_len").over(w_track_id_listener_id_by_ms_p_2)).alias("device_len_change"),
        F.size(F.collect_set("device").over(w_listener_id)).alias("distinct_device_in_listener_id"),
        F.dense_rank().over(w_device_len_device_by_ms_play_2).alias("ms_played_bucket_dense_rank_2"),
        F.lead("device_len").over(w_source_device_len_by_ms_play).alias("next_device_len"),
        F.sum("ms_played_bucket").over(w_source_device_len_by_ms_play_4).alias("ms_played_bucket_sum_2"),
        F.lead("ms_played_bucket", 2).over(w_device_len_device_by_ms_play).alias("next2_ms_played_bucket"),
        (F.col("ms_played_bucket") - F.lag("ms_played_bucket").over(w_track_id_listener_id_by_ms_p_2)).alias("ms_played_bucket_change_2"),
        F.max("ms_played_bucket").over(w_source_device_len_by_ms_play_4).alias("ms_played_bucket_max_2"),
        F.lead("device_len", 1, 0).over(w_source_device_len_by_ms_play).alias("next_device_len_2"),
    )
)

leaders = with_windows.filter(F.col("ms_played_bucket_dense_rank_2") <= 3)

result = (
    leaders
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "ms_played_bucket",
        "device_len",
        "ms_played_bucket_change",
        "ms_played_bucket_rank",
        "ms_played_bucket_dense_rank",
        "device_len_change",
        "ms_played_bucket_dense_rank_2",
        "next_device_len",
        "ms_played_bucket_sum_2",
        "next2_ms_played_bucket",
        "ms_played_bucket_change_2",
        "ms_played_bucket_max_2",
        "next_device_len_2",
    )
    .orderBy("ms_played_bucket", "ms_played_bucket_dense_rank", "id")
    .limit(10)
)
