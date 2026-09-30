from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

with_prev = prepared_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_listener_id_running))

sessions = (
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

labeled = (
    sessions
    .select(
        "*",
        F.date_format(F.col("session_start"), "yyyy-MM").alias("session_start_label"),
        F.date_trunc("quarter", F.col("session_end")).alias("session_end_quarter"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
)

result = (
    labeled
    .rollup("listener_id", "event_count", "burst_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.percentile_approx("duration_min", 0.5).alias("median_duration_min"),
    )
)
