from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
    )
)

with_prev = prepared_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_listener_id_running))

listener_id_stats = (
    sessionized
    .groupBy("listener_id")
    .agg(F.avg("starts_session").alias("avg_starts_session"))
)

vs_group = (
    sessionized.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.starts_session") > F.col("g.avg_starts_session"))
    .select(
        "s.*",
        "g.avg_starts_session",
        F.round(F.col("s.starts_session") - F.col("g.avg_starts_session"), 2).alias("starts_session_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        "prev_played_at",
        "starts_session",
        "session_no",
        "avg_starts_session",
        "starts_session_minus_avg",
    )
    .orderBy(F.col("source").desc(), F.col("track_id").desc())
)
