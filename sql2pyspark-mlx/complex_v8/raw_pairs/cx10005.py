from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.abs(F.col("ms_played") - 176302).alias("ms_played_distance"),
    )
)

with_prev = prepared.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_listener_id_running))

session_summary = (
    sessionized
    .groupBy("listener_id", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

with_parent_listener = (
    session_summary.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select("s.*", F.col("l.birth_year").alias("listener_birth_year"))
)

line_level_plays = (
    with_parent_listener.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.listener_id",
        "s.burst_id",
        "s.session_start",
        "s.events",
        "s.listener_birth_year",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    line_level_plays
    .groupingSets(
        [["events", "listener_id"], ["events"], []],
        "events", "listener_id",
    )
    .agg(
        F.percentile_approx("listener_birth_year", 0.5).alias("median_listener_birth_year"),
        F.max("session_start").alias("last_session_start"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
    )
    .orderBy(F.col("events").asc_nulls_last(), F.col("listener_id").asc_nulls_last())
)
