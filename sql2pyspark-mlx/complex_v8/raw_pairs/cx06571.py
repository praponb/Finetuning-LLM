from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
    )
)

with_prev = valid.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_track_id_running))

screened = (
    sessionized.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") <= 301149) & (F.col("p.source").isNotNull()), "left_anti")
    .filter(F.col("starts_session") >= 0)
)

result = (
    screened
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "prev_played_at",
        "starts_session",
        "session_id",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
    .orderBy(F.col("played_at").desc(), F.col("device_slug").desc())
)
