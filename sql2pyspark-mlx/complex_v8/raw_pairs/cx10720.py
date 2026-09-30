from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_listener_id = Window.partitionBy("listener_id")
w_listener_id_source_by_source = Window.partitionBy("listener_id", "source").orderBy("source_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_source_by_source_2 = Window.partitionBy("listener_id", "source").orderBy("source_len", "id").rowsBetween(-3, Window.currentRow)
w_source_len_device_by_ms_play = Window.partitionBy("source_len", "device").orderBy("ms_played_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_ties = Window.orderBy(F.col("played_at").desc())
w_source_len_device_by_ms_play_2 = Window.partitionBy("source_len", "device").orderBy("ms_played_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_len_device_by_ms_play_3 = Window.partitionBy("source_len", "device").orderBy("ms_played_bucket", "id")
w_source = Window.partitionBy("source")
w_is_skipped_device_by_ms_play = Window.partitionBy("is_skipped", "device").orderBy("ms_played_share_of_listener_id", "id")
w_is_skipped_device_by_ms_play_2 = Window.partitionBy("is_skipped", "device").orderBy("ms_played_share_of_listener_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_device_by_minutes_sin = Window.partitionBy("source", "device").orderBy(F.col("minutes_since_prev_played_at").desc())
w_device_source_by_listener_id = Window.partitionBy("device", "source").orderBy(F.col("listener_id_avg_source_len").desc(), "id")

filtered = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(2, 3))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        (F.col("ms_played_bucket") - F.lag("ms_played_bucket").over(w_all_by_played_at)).alias("ms_played_bucket_change"),
        F.sum("ms_played_bucket").over(w_listener_id).alias("listener_id_sum_ms_played_bucket"),
        F.first("ms_played").over(w_listener_id_source_by_source).alias("first_ms_played"),
        F.max("ms_played").over(w_listener_id_source_by_source_2).alias("ms_played_max_4"),
        F.nth_value("ms_played_bucket", 2).over(w_source_len_device_by_ms_play).alias("second_ms_played_bucket"),
        F.round(F.percent_rank().over(w_all_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.sum("source_len").over(w_source_len_device_by_ms_play_2).alias("running_source_len"),
        F.row_number().over(w_source_len_device_by_ms_play_3).alias("seq_no"),
        F.round(F.avg("source_len").over(w_listener_id), 2).alias("listener_id_avg_source_len"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source_len",
        "ms_played_bucket",
        "ms_played_bucket_change",
        "first_ms_played",
        "played_at_percent_rank",
        "running_source_len",
        "seq_no",
        "minutes_since_prev_played_at",
        "ms_played_share_of_listener_id",
        F.min("ms_played_bucket_change").over(w_source).alias("source_min_ms_played_bucket_change"),
        F.dense_rank().over(w_is_skipped_device_by_ms_play).alias("ms_played_share_of_listener_id_dense_rank"),
        F.first("first_ms_played").over(w_is_skipped_device_by_ms_play_2).alias("first_first_ms_played"),
        F.dense_rank().over(w_source_device_by_minutes_sin).alias("minutes_since_prev_played_at_dense_rank"),
        (F.col("minutes_since_prev_played_at") - F.lag("minutes_since_prev_played_at").over(w_device_source_by_listener_id)).alias("minutes_since_prev_played_at_change"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "ms_played",
        "is_skipped",
        "source_len",
        "ms_played_bucket",
        "running_source_len",
        "seq_no",
        "ms_played_share_of_listener_id",
        "source_min_ms_played_bucket_change",
        "ms_played_share_of_listener_id_dense_rank",
        "first_first_ms_played",
        "minutes_since_prev_played_at_change",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
        F.when(F.col("first_ms_played") >= 299017, "A").when(F.col("first_ms_played") >= 72464, "B").otherwise("C").alias("first_ms_played_tier"),
    )
)
