from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

with_prev = valid.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_source_running))

per_session = (
    sessionized
    .groupBy("source", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("days_since_played_at").alias("session_days_since_played_at"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

totals = (
    per_session
    .groupBy("source", "session_no")
    .agg(
        F.sum("session_days_since_played_at").alias("total_session_days_since_played_at"),
        F.min("session_days_since_played_at").alias("min_session_days_since_played_at"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        F.count("*").alias("n_rows"),
    )
)

without_matches = (
    totals
    .crossJoin(totals.agg(F.avg("avg_duration_min").alias("overall_avg_avg_duration_min")))
    .filter(F.col("avg_duration_min") > F.col("overall_avg_avg_duration_min"))
    .drop("overall_avg_avg_duration_min")
)

result = (
    without_matches
    .groupBy("n_rows", "session_no")
    .pivot("source", ["search", "album", "playlist", "radio"])
    .agg(F.sum("min_session_days_since_played_at"))
    .orderBy("n_rows", "session_no")
)
