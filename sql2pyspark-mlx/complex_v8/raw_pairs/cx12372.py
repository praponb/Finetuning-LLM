from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_prev = base_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_track_id_running))

per_session = (
    sessionized
    .groupBy("track_id", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .select(
        "track_id",
        "burst_id",
        "session_start",
        "session_end",
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
    )
    .orderBy("burst_id", "session_start")
)
