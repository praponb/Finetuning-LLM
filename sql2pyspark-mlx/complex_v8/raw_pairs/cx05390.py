from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

with_prev = filtered_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_listener_id_running))

joined = (
    sessionized.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
    )
)

result = (
    joined
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "prev_played_at",
        "is_new_session",
        "session_id",
        "track_bpm",
        "track_duration_sec",
        F.abs(F.col("session_id") - 2).alias("session_id_distance"),
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
    )
    .orderBy("session_id_distance", F.col("prev_played_at").asc_nulls_last())
)
