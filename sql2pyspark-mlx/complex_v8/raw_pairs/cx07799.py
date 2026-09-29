from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = df_plays

with_prev = prepared.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_device_running))

with_parent = (
    sessionized.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.is_explicit"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.prev_played_at",
        "s.starts_session",
        "s.session_id",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.title").alias("track_title"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "prev_played_at",
        "starts_session",
        F.round(F.col("starts_session") / F.nullif(F.col("session_id"), F.lit(0)), 3).alias("starts_session_per_session_id"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("prev_played_at")).alias("days_since_prev_played_at"),
    )
    .orderBy("days_since_prev_played_at", F.col("track_id").asc_nulls_last())
)
