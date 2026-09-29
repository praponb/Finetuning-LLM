from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_events_by_session_end = Window.partitionBy("events").orderBy("session_end", "device", "session_id")
w_session_id = Window.partitionBy("session_id")
w_events_device_by_session_sta = Window.partitionBy("events", "device").orderBy(F.col("session_start").desc(), "device", "session_id")

valid_plays = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(8, 12))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

with_prev = valid_plays.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_device_running))

session_summary = (
    sessionized
    .groupBy("device", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    session_summary
    .select(
        "*",
        F.row_number().over(w_events_by_session_end).alias("seq_no"),
        F.round(F.col("session_id") - F.avg("session_id").over(w_session_id), 2).alias("session_id_vs_session_id_avg"),
        F.sum("events").over(w_session_id).alias("session_id_sum_events"),
        F.rank().over(w_events_device_by_session_sta).alias("session_start_rank"),
        F.dense_rank().over(w_events_by_session_end).alias("session_end_dense_rank"),
        F.dense_rank().over(w_events_device_by_session_sta).alias("session_start_dense_rank"),
    )
)

leaders = (
    windowed
    .filter((F.col("session_start_dense_rank") == 1) & (F.col("events") <= 5))
    .drop("session_start_dense_rank")
)

result = (
    leaders
    .select(
        "device",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        "seq_no",
        "session_id_vs_session_id_avg",
        "session_id_sum_events",
        "session_start_rank",
        "session_end_dense_rank",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_end")).alias("days_since_session_end"),
        F.round(F.col("session_end_dense_rank") / F.nullif(F.col("seq_no"), F.lit(0)), 3).alias("session_end_dense_rank_per_seq_no"),
    )
    .orderBy(F.col("session_end_dense_rank").desc(), F.col("session_end").desc())
)
