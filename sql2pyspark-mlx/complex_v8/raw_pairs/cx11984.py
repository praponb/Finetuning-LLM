from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_plays = (
    df_plays
    .filter(F.col("source").isin("playlist", "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
    )
)

with_prev = recent_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_listener_id_running))

per_session = (
    sessionized
    .groupBy("listener_id", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

detail_rows_plays = (
    per_session.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.listener_id",
        "s.burst_id",
        "s.session_start",
        "s.session_end",
        "s.event_count",
        "s.duration_min",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

play_track_id_stats = (
    detail_rows_plays
    .groupBy("play_track_id")
    .agg(F.percentile_approx("burst_id", 0.5).alias("median_burst_id"))
)

vs_group = (
    detail_rows_plays.alias("s")
    .join(play_track_id_stats.alias("g"), F.col("g.play_track_id") == F.col("s.play_track_id"), "inner")
    .filter(F.col("s.burst_id") >= F.col("g.median_burst_id"))
    .select(
        "s.*",
        "g.median_burst_id",
        F.round(F.col("s.burst_id") - F.col("g.median_burst_id"), 2).alias("burst_id_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "listener_id",
        "burst_id",
        "session_end",
        "event_count",
        "duration_min",
        "play_id",
        "play_track_id",
        "play_is_skipped",
        "median_burst_id",
        "burst_id_minus_median",
        F.coalesce(F.col("median_burst_id"), F.lit(0.0)).alias("median_burst_id_filled"),
    )
)
