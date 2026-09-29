from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_plays
    .filter(F.col("ms_played") <= 221269)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

with_prev = scoped.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_listener_id_running))

session_summary = (
    sessionized
    .groupBy("listener_id", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

detail_rows = (
    session_summary.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.listener_id",
        "s.session_no",
        "s.session_start",
        "s.event_count",
        "s.duration_min",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    detail_rows
    .groupingSets(
        [["listener_id", "play_id"], ["listener_id"], ["play_id"], []],
        "listener_id", "play_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("play_ms_played"), 2).alias("stddev_play_ms_played"),
        F.round(F.stddev("event_count"), 2).alias("stddev_event_count"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("play_id").asc_nulls_last(),
    )
)
