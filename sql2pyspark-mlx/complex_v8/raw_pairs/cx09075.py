from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_plays
    .filter(F.col("ms_played").between(95960, 282311))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
    )
)

with_prev = filtered.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_listener_id_running))

session_summary = (
    sessionized
    .groupBy("listener_id", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .filter(F.to_date(F.col("session_start")) < F.lit("2025-02-13").cast("date"))
    .select(
        "listener_id",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "session_ms_played",
        "duration_min",
    )
    .orderBy(F.col("session_end").asc(), F.col("session_ms_played").asc_nulls_last())
)
