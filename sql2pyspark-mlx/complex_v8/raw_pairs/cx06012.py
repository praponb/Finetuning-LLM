from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .filter(F.col("track_id") >= 29)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
    )
)

with_prev = valid.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_track_id_running))

session_summary = (
    sessionized
    .groupBy("track_id", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

summary = (
    session_summary
    .filter(F.col("duration_min") > 72.0)
    .groupBy(
        F.date_trunc("month", "session_end").alias("session_end_month"),
        "n_events",
    )
    .agg(
        F.min("session_end").alias("first_session_end"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.max("n_events").alias("max_n_events"),
        F.round(F.sum("n_events") / F.nullif(F.sum("session_id"), F.lit(0)), 3).alias("n_events_to_session_id_ratio"),
        F.percentile_approx("session_id", 0.5).alias("median_session_id"),
    )
)

result = (
    summary
    .filter(F.to_date(F.col("first_session_end")) < F.lit("2025-01-25").cast("date"))
    .select(
        "session_end_month",
        "n_events",
        "distinct_track_ids",
        "max_n_events",
        "median_session_id",
        (F.floor(F.col("distinct_track_ids") / 2) * 2).alias("distinct_track_ids_bucket"),
    )
    .orderBy(F.col("max_n_events").desc())
)
