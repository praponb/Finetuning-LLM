from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at_plus_15m", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at_plus_15m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_plus_15m_to_date_b = Window.partitionBy(F.to_date("played_at_plus_15m")).orderBy("ms_played", "id").rowsBetween(-2, -1)
w_played_at_plus_15m_month_ses = Window.partitionBy(F.date_trunc("month", "played_at_plus_15m"), "session_id")
w_played_at_plus_15m_to_date_b_2 = Window.partitionBy(F.to_date("played_at_plus_15m")).orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_plus_15m_to_date_b_3 = Window.partitionBy(F.to_date("played_at_plus_15m")).orderBy("ms_played", "id")

cleaned_plays = (
    df_plays
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 15 MINUTES")).alias("played_at_plus_15m"),
    )
)

with_prev = cleaned_plays.withColumn("prev_played_at_plus_15m", F.lag("played_at_plus_15m").over(w_source))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at_plus_15m").isNull() | (F.col("played_at_plus_15m") > F.col("prev_played_at_plus_15m") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_source_running))

with_windows = (
    sessionized
    .select(
        "id",
        "listener_id",
        "ms_played",
        "is_skipped",
        "source",
        "played_at_plus_15m",
        F.round(F.avg("ms_played").over(w_played_at_plus_15m_to_date_b), 2).alias("ms_played_avg_2_prior"),
        F.round(F.avg("starts_session").over(w_played_at_plus_15m_month_ses), 2).alias("played_at_plus_15m_month_session_id_avg_starts_s"),
        F.max("session_id").over(w_played_at_plus_15m_to_date_b_2).alias("max_session_id_so_far"),
        F.lead("starts_session", 2, 0).over(w_played_at_plus_15m_to_date_b_3).alias("next2_starts_session"),
    )
)

result = (
    with_windows
    .groupBy("is_skipped", "played_at_plus_15m")
    .pivot("source", ["radio", "playlist", "search", "album"])
    .agg(F.count("ms_played_avg_2_prior"))
)
