from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at_plus_7d", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at_plus_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_plays
    .filter(F.col("track_id") >= 16)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
    )
)

with_prev = prepared.withColumn("prev_played_at_plus_7d", F.lag("played_at_plus_7d").over(w_track_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at_plus_7d").isNull() | (F.col("played_at_plus_7d") > F.col("prev_played_at_plus_7d") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_track_id_running))

scored = (
    sessionized
    .filter(F.col("source").isin("album", "radio", "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "played_at_plus_7d",
        "starts_session",
        F.round(F.col("ms_played") / F.nullif(F.col("session_no"), F.lit(0)), 3).alias("ms_played_per_session_no"),
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

result = (
    scored
    .select("played_at_plus_7d")
    .distinct()
)
