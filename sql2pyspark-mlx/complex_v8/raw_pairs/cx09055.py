from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

with_prev = recent.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_listener_id_running))

session_summary = (
    sessionized
    .groupBy("listener_id", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("days_since_played_at").alias("session_days_since_played_at"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

with_parent_listener = (
    session_summary.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    with_parent_listener
    .select(
        "listener_id",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        "listener_plan",
        "listener_username",
        F.round(F.col("duration_min") * 1.1, 2).alias("duration_min_adj"),
        F.length(F.trim(F.col("listener_username"))).alias("listener_username_len"),
    )
    .orderBy(F.col("session_start").asc_nulls_last())
)
