from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 308104, "HIGH").when(F.col("ms_played") >= 151112, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
)

with_prev = recent_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_track_id_running))

joined = (
    sessionized.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.source",
        "s.ms_played_tier",
        "s.prev_played_at",
        "s.is_new_session",
        "s.session_no",
        F.col("t.title").alias("track_title"),
    )
)

result = (
    joined
    .filter(F.col("is_new_session") > 0)
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "ms_played_tier",
        "prev_played_at",
        "session_no",
        "track_title",
        F.length(F.trim(F.col("ms_played_tier"))).alias("ms_played_tier_len"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)
