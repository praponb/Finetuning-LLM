from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_cnt_by_cnt = Window.partitionBy("is_skipped", "cnt").orderBy("cnt", "is_skipped", "listener_id")
w_is_skipped_cnt_by_cnt_runnin = Window.partitionBy("is_skipped", "cnt").orderBy("cnt", "is_skipped", "listener_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")

scoped_plays = (
    df_plays
    .filter(~F.col("source").isin("playlist", "album"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("|", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

per_group = (
    scoped_plays
    .groupBy("is_skipped", "listener_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
    )
)

metrics = (
    per_group
    .select(
        "*",
        F.round(F.cume_dist().over(w_is_skipped_cnt_by_cnt), 3).alias("cnt_cume_dist"),
        F.first("cnt", ignorenulls=True).over(w_is_skipped_cnt_by_cnt_runnin).alias("first_known_cnt"),
        F.min("avg_ms_played").over(w_listener_id).alias("listener_id_min_avg_ms_played"),
        F.min("distinct_ids").over(w_listener_id).alias("listener_id_min_distinct_ids"),
        F.round(F.col("cnt") / F.nullif(F.sum("cnt").over(w_listener_id), F.lit(0)), 4).alias("cnt_share_of_listener_id"),
    )
)

detail_rows_plays = (
    metrics.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.is_skipped",
        "s.listener_id",
        "s.cnt",
        "s.avg_ms_played",
        "s.listener_id_min_avg_ms_played",
        "s.listener_id_min_distinct_ids",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.track_id").alias("play_track_id"),
    )
)

result = (
    detail_rows_plays
    .select(
        "is_skipped",
        "listener_id",
        "cnt",
        "avg_ms_played",
        "listener_id_min_avg_ms_played",
        "listener_id_min_distinct_ids",
        "play_id",
        "play_device",
        "play_track_id",
        F.when(F.col("avg_ms_played") >= 281500.0, "large").when(F.col("avg_ms_played") >= 143250.0, "medium").otherwise("small").alias("avg_ms_played_tier"),
    )
    .orderBy("play_track_id")
)
