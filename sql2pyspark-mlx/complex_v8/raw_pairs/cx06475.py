from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

with_prev = prepared.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_listener_id_running))

track_id_stats = (
    sessionized
    .groupBy("track_id")
    .agg(F.max("is_new_session").alias("max_is_new_session"))
)

vs_group = (
    sessionized.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.is_new_session") == F.col("g.max_is_new_session"))
    .select(
        "s.*",
        "g.max_is_new_session",
        F.round(F.col("s.is_new_session") - F.col("g.max_is_new_session"), 2).alias("is_new_session_minus_max"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "prev_played_at",
        "burst_id",
        "max_is_new_session",
        "is_new_session_minus_max",
    )
    .orderBy("ms_played")
)
