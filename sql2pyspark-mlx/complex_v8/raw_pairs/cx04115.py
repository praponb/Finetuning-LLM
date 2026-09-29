from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.when(F.col("device").isin("phone", "car"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group"),
    )
)

with_prev = base.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_listener_id_running))

session_summary = (
    sessionized
    .groupBy("listener_id", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    session_summary.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") >= 56848) & (F.col("p.track_id").isNotNull()), "left_anti")
)

result = (
    with_activity
    .select(
        "listener_id",
        "session_start",
        "session_end",
        "n_events",
        F.abs(F.col("session_ms_played") - 882250).alias("session_ms_played_distance"),
        F.when(F.col("n_events") >= 5, "large").when(F.col("n_events") >= 3, "medium").otherwise("small").alias("n_events_tier"),
    )
)
