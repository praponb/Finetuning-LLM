from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_track_id = Window.partitionBy("source", "track_id")
w_listener_id_by_ms_played_mov = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_listener_id_by_ms_played_ful = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_plays
    .filter(F.col("ms_played") < 333795)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id") <= 7), "left_anti")
)

with_trends = (
    filtered_2
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "source_device_label",
        "ms_played_bucket",
        F.size(F.collect_set("device").over(w_source_track_id)).alias("distinct_device_in_source_track_id"),
        F.sum("ms_played").over(w_listener_id_by_ms_played_mov).alias("ms_played_sum_3"),
        F.ntile(4).over(w_listener_id_by_ms_played).alias("ms_played_quartile"),
        F.count(F.lit(1)).over(w_source_track_id).alias("rows_in_source_track_id"),
        F.nth_value("ms_played_bucket", 2).over(w_listener_id_by_ms_played_ful).alias("second_ms_played_bucket"),
    )
)

result = (
    with_trends
    .rollup("source", "ms_played_quartile")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("rows_in_source_track_id"), 2).alias("stddev_rows_in_source_track_id"),
    )
    .orderBy(
        F.col("source").asc_nulls_last(),
        F.col("ms_played_quartile").asc_nulls_last(),
    )
)
