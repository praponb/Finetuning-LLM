from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
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

with_prev = filtered_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_track_id_running))

per_session = (
    sessionized
    .groupBy("track_id", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

detail_rows_plays = (
    per_session.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.track_id",
        "s.session_id",
        "s.events",
        "s.session_ms_played",
        "s.duration_min",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    detail_rows_plays
    .select(
        "track_id",
        "session_id",
        "events",
        "session_ms_played",
    )
    .unpivot(
        ids=["track_id", "session_id"],
        values=["events", "session_ms_played"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("track_id", "session_id", "metric_name")
)
