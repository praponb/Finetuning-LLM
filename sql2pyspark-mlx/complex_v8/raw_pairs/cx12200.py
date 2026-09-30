from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_new_session_track_id = Window.partitionBy("is_new_session", "track_id")
w_track_id_by_prev_played_at = Window.partitionBy("track_id").orderBy(F.col("prev_played_at").desc(), "id")

scoped_plays = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_listener_id_running))

sequenced = (
    sessionized
    .select(
        "*",
        F.size(F.collect_set("device").over(w_is_new_session_track_id)).alias("distinct_device_in_is_new_session_track_id"),
        F.ntile(5).over(w_track_id_by_prev_played_at).alias("prev_played_at_quintile"),
        F.lag("is_new_session", 3).over(w_track_id_by_prev_played_at).alias("prev3_is_new_session"),
        F.size(F.collect_set("listener_id").over(w_is_new_session_track_id)).alias("distinct_listener_id_in_is_new_session_track_id"),
    )
)

detail_rows = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.track_id") >= 13), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.is_new_session",
        "s.distinct_device_in_is_new_session_track_id",
        "s.prev_played_at_quintile",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    detail_rows
    .rollup("is_new_session", "distinct_device_in_is_new_session_track_id")
    .agg(F.countDistinct("play_track_id").alias("distinct_play_track_ids"))
    .orderBy(
        F.col("is_new_session").asc_nulls_last(),
        F.col("distinct_device_in_is_new_session_track_id").asc_nulls_last(),
    )
)
