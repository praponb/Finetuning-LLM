from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_listener_id_running))

sessions = (
    sessionized
    .groupBy("listener_id", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched_listener = (
    sessions.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.listener_id",
        "s.session_no",
        "s.session_start",
        "s.session_end",
        "s.events",
        "s.duration_min",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    enriched_listener
    .select(
        "listener_id",
        "session_no",
        "session_end",
        "events",
        "duration_min",
        F.when(F.col("duration_min") >= 168.0, "large").when(F.col("duration_min") >= 48.0, "medium").otherwise("small").alias("duration_min_tier"),
        F.round(F.col("duration_min") * 1.5, 2).alias("duration_min_adj"),
    )
)
