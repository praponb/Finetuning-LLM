from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_plays = (
    df_plays
    .filter(F.col("ms_played") <= 261579)
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

with_prev = cleaned_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_track_id_running))

session_summary = (
    sessionized
    .groupBy("track_id", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = (
    session_summary
    .select(
        "*",
        F.date_trunc("week", F.col("session_end")).alias("session_end_week"),
        F.when(F.col("events") >= 4, "HIGH").when(F.col("events") >= 2, "MEDIUM").otherwise("LOW").alias("events_tier"),
        (F.col("events") - F.col("duration_min")).alias("events_minus_duration_min"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.sum("ms_played").alias("total_play_ms_played"),
        F.count("*").alias("play_count"),
        F.max("ms_played").alias("max_play_ms_played"),
    )
)

with_counts = (
    tagged.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.total_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.max_play_ms_played",
    )
)

result = (
    with_counts
    .select(
        "track_id",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "events_tier",
        "events_minus_duration_min",
        "total_play_ms_played",
        "play_count",
        "max_play_ms_played",
    )
    .orderBy(F.col("track_id").asc(), "events_tier")
)
