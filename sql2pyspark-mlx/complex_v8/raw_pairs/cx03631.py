from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_ms_played_run = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_source = Window.partitionBy("source")
w_listener_id_by_ms_played_ful = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_device_by_ms_play = Window.partitionBy("is_skipped", "device").orderBy(F.col("ms_played").desc(), "id")
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy("ms_played", "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_track_id_by_ms_played_full = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_ms_played_tie = Window.partitionBy("listener_id").orderBy("ms_played")
w_is_skipped_device_by_ms_play_2 = Window.partitionBy("is_skipped", "device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

windowed = (
    scoped
    .select(
        "id",
        "track_id",
        "is_skipped",
        "source",
        F.last("ms_played", ignorenulls=True).over(w_listener_id_by_ms_played_run).alias("last_known_ms_played"),
        F.row_number().over(w_listener_id_by_ms_played).alias("ms_played_seq"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_source), F.lit(0)), 4).alias("ms_played_share_of_source"),
        F.first("ms_played").over(w_listener_id_by_ms_played_ful).alias("first_ms_played"),
        F.dense_rank().over(w_is_skipped_device_by_ms_play).alias("ms_played_dense_rank"),
        F.ntile(10).over(w_is_skipped_by_ms_played).alias("ms_played_decile"),
        F.size(F.collect_set("listener_id").over(w_source)).alias("distinct_listener_id_in_source"),
        F.size(F.collect_set("listener_id").over(w_is_skipped)).alias("distinct_listener_id_in_is_skipped"),
        F.nth_value("ms_played", 2).over(w_track_id_by_ms_played_full).alias("second_ms_played"),
        (F.col("ms_played") - F.lag("ms_played").over(w_listener_id_by_ms_played)).alias("ms_played_change"),
        F.first("ms_played", ignorenulls=True).over(w_listener_id_by_ms_played_run).alias("first_known_ms_played"),
        F.round(F.cume_dist().over(w_listener_id_by_ms_played_tie), 3).alias("ms_played_cume_dist"),
        F.sum("ms_played").over(w_is_skipped).alias("is_skipped_sum_ms_played"),
        F.first("ms_played", ignorenulls=True).over(w_is_skipped_device_by_ms_play_2).alias("first_known_ms_played_2"),
    )
)

result = (
    windowed
    .filter(F.col("last_known_ms_played") < 246779)
    .select(
        "id",
        "track_id",
        "source",
        "last_known_ms_played",
        "ms_played_seq",
        "ms_played_share_of_source",
        "first_ms_played",
        "ms_played_dense_rank",
        "ms_played_decile",
        "distinct_listener_id_in_source",
        "distinct_listener_id_in_is_skipped",
        "second_ms_played",
        "ms_played_change",
        "first_known_ms_played",
        "ms_played_cume_dist",
        "is_skipped_sum_ms_played",
    )
    .orderBy(F.col("ms_played_dense_rank").desc())
)
