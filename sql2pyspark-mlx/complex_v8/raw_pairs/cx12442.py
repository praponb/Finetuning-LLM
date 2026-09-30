from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_plays = (
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
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

with_prev = filtered_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_track_id_running))

session_summary = (
    sessionized
    .groupBy("track_id", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    session_summary
    .select(
        "*",
        (F.col("session_end") + F.expr("INTERVAL 2 HOURS")).alias("session_end_plus_2h"),
        F.date_trunc("day", F.col("session_start")).alias("session_start_day"),
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
    )
)

exploded_detail = (
    annotated.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.track_id",
        "s.session_no",
        "s.session_start",
        "s.session_end",
        "s.n_events",
        "s.duration_min",
        "s.session_end_plus_2h",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    exploded_detail
    .groupBy("session_start", "play_listener_id")
    .pivot("play_device", ["car", "phone", "tv", "desktop"])
    .agg(F.sum("n_events"))
    .orderBy("session_start", "play_listener_id")
)
