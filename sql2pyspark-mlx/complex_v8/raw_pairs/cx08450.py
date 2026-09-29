from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_year_listener_id = Window.partitionBy(F.year("session_end"), "listener_id").orderBy("duration_min", "listener_id", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_count_running = Window.orderBy("event_count", "listener_id", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_plays
    .filter(F.col("source") == "album")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

with_prev = recent.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_listener_id_running))

per_session = (
    sessionized
    .groupBy("listener_id", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("source_len").alias("session_source_len"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    per_session.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device").isNotNull()), "left_anti")
    .filter(F.col("session_start") >= F.lit("2025-01-17 09:00:00").cast("timestamp"))
)

with_windows = (
    with_activity
    .withColumn("first_known_duration_min", F.first("duration_min", ignorenulls=True).over(w_session_end_year_listener_id))
    .withColumn("last_known_session_source_len", F.last("session_source_len", ignorenulls=True).over(w_all_by_event_count_running))
)

result = (
    with_windows
    .filter(F.col("session_end") < F.lit("2025-02-17 09:00:00").cast("timestamp"))
    .select(
        "listener_id",
        "session_id",
        "session_end",
        "event_count",
        "session_source_len",
        "duration_min",
        "first_known_duration_min",
    )
    .orderBy(F.col("event_count").asc(), F.col("session_source_len").desc())
)
