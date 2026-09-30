from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
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

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_track_id_running))

per_session = (
    sessionized
    .groupBy("track_id", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("ms_played_bucket").alias("session_ms_played_bucket"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    per_session
    .filter(F.col("burst_id") > 2)
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 60, 1).alias("minutes_session_start_to_session_end"),
        F.date_trunc("day", F.col("session_end")).alias("session_end_day"),
    )
)

enriched = derived.withColumn("duration_min_bucket", F.floor(F.col("duration_min") / 50) * 50)

result = (
    enriched
    .select(
        "track_id",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "session_ms_played_bucket",
        "duration_min",
        "session_end_day",
        "duration_min_bucket",
    )
    .orderBy(F.col("events").desc())
)
