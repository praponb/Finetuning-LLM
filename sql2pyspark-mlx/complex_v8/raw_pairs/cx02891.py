from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")

valid_plays = (
    df_plays
    .filter((F.col("device") == "desktop") & (F.col("listener_id") >= 6))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.concat_ws("|", F.col("source"), F.col("device")).alias("source_device_label"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
        F.concat_ws("|", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

ranked = (
    valid_plays
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_track_id), 2).alias("track_id_avg_ms_played"),
        F.lag("played_at").over(w_all_by_played_at).alias("prev_played_at"),
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id"),
        F.dense_rank().over(w_all_by_played_at).alias("played_at_dense_rank"),
    )
)

result = (
    ranked
    .cube("device", "distinct_listener_id_in_track_id")
    .agg(
        F.grouping("device").alias("is_total_device"),
        F.round(F.sum("track_id_avg_ms_played") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("track_id_avg_ms_played_to_ms_played_ratio"),
    )
)
