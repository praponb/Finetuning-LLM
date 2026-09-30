from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

with_prev = valid_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_listener_id_running))

result = (
    sessionized
    .cube("session_id", "listener_id", "track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.max("session_id").alias("max_session_id"),
    )
    .orderBy(
        F.col("session_id").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
    )
)
