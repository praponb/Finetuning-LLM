from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_device_source_by_ms_played = Window.partitionBy("device", "source").orderBy("ms_played", "id")
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_ties = Window.orderBy(F.col("ms_played").desc())

valid_plays = (
    df_plays
    .filter(F.col("ms_played") > 120663)
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

with_parent = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("l.username").alias("listener_username"),
        F.col("l.interests").alias("listener_interests"),
    )
)

with_trends = (
    with_parent
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "listener_interests",
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.lead("ms_played", 3).over(w_all_by_ms_played).alias("next3_ms_played"),
        F.ntile(4).over(w_device_source_by_ms_played).alias("ms_played_quartile"),
        F.lead("ms_played").over(w_listener_id_by_ms_played).alias("next_ms_played"),
        (F.col("ms_played") - F.lag("ms_played").over(w_listener_id_by_ms_played)).alias("ms_played_change"),
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id"),
        F.min("ms_played").over(w_all_by_ms_played_running).alias("min_ms_played_so_far"),
        F.lead("ms_played").over(w_all_by_ms_played).alias("next_ms_played_2"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id), 2).alias("ms_played_vs_track_id_avg"),
        F.round(F.cume_dist().over(w_all_by_ms_played_ties), 3).alias("ms_played_cume_dist"),
        F.lead("ms_played", 2).over(w_listener_id_by_ms_played).alias("next2_ms_played"),
    )
)

tagged = with_trends.withColumn("ms_played_bucket", F.floor(F.col("ms_played") / 50000) * 50000)

result = (
    tagged
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "source",
        "rows_in_track_id",
        "next3_ms_played",
        "next_ms_played",
        "ms_played_change",
        "distinct_listener_id_in_track_id",
        "min_ms_played_so_far",
        "ms_played_vs_track_id_avg",
        "ms_played_cume_dist",
        "next2_ms_played",
        "ms_played_bucket",
        (F.col("next_ms_played") + F.col("ms_played_cume_dist")).alias("next_ms_played_plus_ms_played_cume_dist"),
    )
    .orderBy(F.col("next3_ms_played").desc_nulls_first())
)
