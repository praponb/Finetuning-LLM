from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .filter(F.col("source") == "album")
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

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_source_running))

session_summary = (
    sessionized
    .groupBy("source", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    session_summary
    .filter(F.month(F.col("session_end")).isin(1, 2))
    .select(
        "source",
        "session_no",
        "session_start",
        "n_events",
        "duration_min",
        F.least(F.col("n_events"), F.col("session_ms_played")).alias("least_n_events_session_ms_played"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

result = (
    derived
    .select(
        "source",
        "session_no",
        "session_start",
        "n_events",
        "least_n_events_session_ms_played",
        (F.col("session_start") + F.expr("INTERVAL 7 DAYS")).alias("session_start_plus_7d"),
        F.abs(F.col("n_events") - 3).alias("n_events_distance"),
    )
)
