from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_session_end_month = Window.partitionBy("burst_id", F.date_trunc("month", "session_end"))
w_source_event_count_by_durati = Window.partitionBy("source", "event_count").orderBy(F.col("duration_min").asc_nulls_last(), "source", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_end_month_by_duratio = Window.partitionBy(F.date_trunc("month", "session_end")).orderBy(F.col("duration_min").desc())

scoped_plays = df_plays

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_source_running))

session_summary = (
    sessionized
    .groupBy("source", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    session_summary
    .select(
        "source",
        "burst_id",
        "session_end",
        "session_ms_played",
        "duration_min",
        F.sum("session_ms_played").over(w_burst_id_session_end_month).alias("burst_id_session_end_month_sum_session_ms_played"),
        F.sum("duration_min").over(w_source_event_count_by_durati).alias("running_duration_min"),
        F.round(F.percent_rank().over(w_session_end_month_by_duratio), 3).alias("duration_min_percent_rank"),
    )
)

filtered = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("running_duration_min").alias("overall_avg_running_duration_min")))
    .filter(F.col("running_duration_min") > F.col("overall_avg_running_duration_min"))
    .drop("overall_avg_running_duration_min")
)

result = (
    filtered
    .groupBy("burst_id", "session_end")
    .pivot("source", ["radio", "search", "album"])
    .agg(F.max("session_ms_played"))
)
