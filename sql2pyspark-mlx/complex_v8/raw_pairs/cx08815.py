from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_plays = (
    df_plays
    .filter(F.col("listener_id") >= 6)
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

with_prev = filtered_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_listener_id_running))

session_summary = (
    sessionized
    .groupBy("listener_id", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

with_children_plays = (
    session_summary.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.listener_id",
        "s.burst_id",
        "s.session_start",
        "s.session_ms_played",
        "s.duration_min",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

detail_rows = (
    with_children_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.listener_id",
        "s.burst_id",
        "s.session_start",
        "s.session_ms_played",
        "s.duration_min",
        "s.play_id",
        "s.play_source",
        "s.play_track_id",
        F.col("p.id").alias("play_id_2"),
        F.col("p.ms_played").alias("play_ms_played_2"),
        F.col("p.played_at").alias("play_played_at_2"),
    )
)

result = (
    detail_rows
    .select(
        "listener_id",
        "burst_id",
        "session_ms_played",
        "duration_min",
        "play_source",
        "play_track_id",
        "play_id_2",
        "play_ms_played_2",
        "play_played_at_2",
        F.coalesce(F.col("play_source"), F.lit("UNKNOWN")).alias("play_source_or_unknown"),
        F.length(F.trim(F.col("play_source"))).alias("play_source_len"),
    )
    .orderBy(F.col("play_ms_played_2").desc_nulls_first())
)
