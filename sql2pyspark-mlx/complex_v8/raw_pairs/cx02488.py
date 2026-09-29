from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_burst_id_by_sess = Window.partitionBy("event_count", "burst_id").orderBy("session_end", "listener_id", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_session_start = Window.orderBy("session_start", "listener_id", "burst_id")

scoped = df_plays.filter(F.col("ms_played") >= 168782)

with_prev = scoped.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_listener_id_running))

per_session = (
    sessionized
    .groupBy("listener_id", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    per_session
    .select(
        "*",
        F.nth_value("duration_min", 2).over(w_event_count_burst_id_by_sess).alias("second_duration_min"),
        F.lag("event_count", 3).over(w_all_by_session_start).alias("prev3_event_count"),
    )
)

result = (
    with_windows
    .select(
        "listener_id",
        "burst_id",
        "session_start",
        "event_count",
        "session_ms_played",
        "second_duration_min",
        "prev3_event_count",
        F.round(F.col("duration_min") * 1.1, 2).alias("duration_min_adj"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
)
