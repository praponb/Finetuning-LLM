from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .filter(F.col("ms_played") <= 214343)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
    )
)

with_prev = valid.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_track_id_running))

with_parent_track = (
    sessionized.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("l.interests").alias("listener_interests"),
    )
)

grouped = (
    with_parent_track
    .groupBy("track_id")
    .agg(
        F.round(F.avg("track_duration_sec"), 2).alias("avg_track_duration_sec"),
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
        F.max("prev_played_at").alias("last_prev_played_at"),
        F.sum(F.when(~F.col("is_skipped"), F.col("track_duration_sec"))).alias("track_duration_sec_is_skipped_false"),
    )
)

result = (
    grouped
    .select(
        "track_id",
        "avg_track_duration_sec",
        "n_is_skipped_true",
        "last_prev_played_at",
        "track_duration_sec_is_skipped_false",
    )
    .orderBy(
        F.col("track_duration_sec_is_skipped_false").desc(),
        F.col("avg_track_duration_sec").desc_nulls_first(),
    )
)
