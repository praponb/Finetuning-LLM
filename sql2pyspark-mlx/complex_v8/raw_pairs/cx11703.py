from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = df_plays.filter(F.col("track_id") <= 2)

with_prev = filtered.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_listener_id_running))

with_parent = (
    sessionized.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.bpm") < 156), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.prev_played_at",
        F.col("t.release_date").alias("track_release_date"),
        F.col("l.interests").alias("listener_interests"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        "listener_interests",
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played").cast("string")).alias("source_ref"),
    )
    .orderBy(F.col("ms_played").desc_nulls_first())
)
