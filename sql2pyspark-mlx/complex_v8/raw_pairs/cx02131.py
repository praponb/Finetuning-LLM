from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.year(F.col("played_at")).alias("played_at_year"),
    )
)

with_prev = filtered.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_track_id_running))

per_session = (
    sessionized
    .groupBy("track_id", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

qualified = (
    per_session.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") >= 4) & (~F.col("p.device").isin("phone", "speaker")), "left_anti")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.max("played_at").alias("last_played_at_2"),
        F.min("played_at").alias("first_played_at"),
    )
)

with_counts = (
    qualified.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        "p.last_played_at_2",
        "p.first_played_at",
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
        "duration_min",
        "last_played_at",
        "last_played_at_2",
        "first_played_at",
        F.when(F.col("events") >= 5, "A").when(F.col("events") >= 2, "B").otherwise("C").alias("events_tier"),
    )
)
