from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_listener_id_ms_played_tercil = Window.partitionBy("listener_id", "ms_played_tercile").orderBy(F.col("ms_played_tercile").asc_nulls_last(), "id")
w_is_skipped = Window.partitionBy("is_skipped")

base_plays = (
    df_plays
    .filter(F.col("source") == "playlist")
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
        F.concat_ws(" / ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

windowed = (
    base_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.ntile(3).over(w_all_by_ms_played).alias("ms_played_tercile"),
        F.dense_rank().over(w_all_by_ms_played).alias("ms_played_dense_rank"),
        F.lag("ms_played", 3, 0).over(w_all_by_ms_played).alias("prev3_ms_played"),
    )
)

with_trends = (
    windowed
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "ms_played_tercile",
        "ms_played_dense_rank",
        "prev3_ms_played",
        F.row_number().over(w_listener_id_ms_played_tercil).alias("ms_played_tercile_seq"),
        F.size(F.collect_set("source").over(w_is_skipped)).alias("distinct_source_in_is_skipped"),
    )
)

result = (
    with_trends
    .rollup("device", "track_id")
    .agg(F.array_sort(F.collect_set("device")).alias("device_list"))
)
