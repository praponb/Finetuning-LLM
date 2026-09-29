from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_plays = (
    df_plays
    .filter(F.col("ms_played") >= 160264)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

with_prev = valid_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_source_running))

per_session = (
    sessionized
    .groupBy("source", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = (
    per_session
    .filter(F.col("source").isin("radio", "search", "playlist"))
    .select(
        "source",
        "burst_id",
        "session_end",
        "n_events",
        (F.col("n_events") - F.col("burst_id")).alias("n_events_minus_burst_id"),
        F.to_date(F.col("session_start")).alias("session_start_date"),
    )
)

qualified = (
    tagged
    .crossJoin(tagged.agg(F.avg("n_events").alias("overall_avg_n_events")))
    .filter(F.col("n_events") > F.col("overall_avg_n_events"))
    .drop("overall_avg_n_events")
)

result = (
    qualified
    .filter(F.col("session_start_date") >= F.lit("2024-05-01").cast("date"))
    .select(
        "source",
        "session_end",
        "n_events",
        "n_events_minus_burst_id",
        "session_start_date",
    )
    .orderBy(F.col("session_end").asc_nulls_last(), "n_events_minus_burst_id")
)
