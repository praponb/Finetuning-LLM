from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_2 = Window.partitionBy("source")
w_session_id_by_session_end_pr = Window.partitionBy("session_id").orderBy(F.col("session_end").desc(), "source", "session_id").rowsBetween(-6, -1)
w_all_by_events_running = Window.orderBy(F.col("events").desc(), "source", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_events_ties = Window.orderBy(F.col("events").desc())
w_all_by_events = Window.orderBy(F.col("events").desc(), "source", "session_id")
w_all_by_events_moving_3 = Window.orderBy(F.col("events").desc(), "source", "session_id").rowsBetween(-3, Window.currentRow)

base = (
    df_plays
    .filter(F.col("track_id") <= 28)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.when(F.col("source").isin("album", "radio"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
    )
)

with_prev = base.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_source_running))

session_summary = (
    sessionized
    .groupBy("source", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

metrics = (
    session_summary
    .select(
        "*",
        F.round(F.col("duration_min") - F.avg("duration_min").over(w_source_2), 2).alias("duration_min_vs_source_avg"),
        F.round(F.avg("session_id").over(w_session_id_by_session_end_pr), 2).alias("session_id_avg_6_prior"),
        F.first("duration_min", ignorenulls=True).over(w_all_by_events_running).alias("first_known_duration_min"),
        F.round(F.percent_rank().over(w_all_by_events_ties), 3).alias("events_percent_rank"),
        F.lead("session_end").over(w_all_by_events).alias("next_session_end"),
        F.round(F.avg("events").over(w_all_by_events_moving_3), 2).alias("events_avg_4"),
    )
)

with_activity = (
    metrics
    .crossJoin(metrics.agg(F.avg("first_known_duration_min").alias("overall_avg_first_known_duration_min")))
    .filter(F.col("first_known_duration_min") > F.col("overall_avg_first_known_duration_min"))
    .drop("overall_avg_first_known_duration_min")
)

result = (
    with_activity
    .select(
        "source",
        "session_id",
        "session_end",
        "duration_min",
        "duration_min_vs_source_avg",
        "session_id_avg_6_prior",
        "first_known_duration_min",
        "events_percent_rank",
        "events_avg_4",
    )
)
