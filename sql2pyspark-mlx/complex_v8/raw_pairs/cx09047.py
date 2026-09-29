from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
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
    )
)

with_prev = cleaned.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_listener_id_running))

session_summary = (
    sessionized
    .groupBy("listener_id", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .select(
        "listener_id",
        "session_id",
        "session_end",
        "events",
        "session_ms_played",
        "duration_min",
        F.date_trunc("day", F.col("session_start")).alias("session_start_day"),
        F.round(F.col("session_id") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("session_id_per_duration_min"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("session_start_day").desc(),
    )
)
