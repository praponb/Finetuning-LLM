from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 223828).alias("ms_played_distance"),
    )
)

with_prev = scoped.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_listener_id_running))

detail_rows_plays = (
    sessionized.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.month(F.col("p.played_at")).isin(1, 3)), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.ms_played_distance",
        "s.prev_played_at",
        "s.new_session_flag",
        "s.session_id",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    detail_rows_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source",
        "ms_played_distance",
        "prev_played_at",
        "new_session_flag",
        "session_id",
        "play_played_at",
        "play_listener_id",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
    )
)
