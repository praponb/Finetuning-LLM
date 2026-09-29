from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_plays = df_plays.filter(F.col("ms_played") <= 225733)

with_prev = filtered_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_track_id_running))

session_summary = (
    sessionized
    .groupBy("track_id", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    session_summary
    .groupBy(
        "burst_id",
        F.date_trunc("month", "session_start").alias("session_start_month"),
        F.date_trunc("month", "session_end").alias("session_end_month"),
    )
    .agg(
        F.sum("events").alias("total_events"),
        F.percentile_approx("events", 0.5).alias("median_events"),
        F.round(F.stddev("burst_id"), 2).alias("stddev_burst_id"),
        F.max("session_end").alias("last_session_end"),
    )
)

result = (
    stats
    .select(
        "burst_id",
        "session_start_month",
        "session_end_month",
        "stddev_burst_id",
        "last_session_end",
        F.round(F.col("stddev_burst_id") * 1.5, 2).alias("stddev_burst_id_adj"),
    )
    .orderBy(F.col("stddev_burst_id").desc(), "session_end_month")
)
