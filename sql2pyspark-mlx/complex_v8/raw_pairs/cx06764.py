from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_by_n_events = Window.partitionBy("burst_id").orderBy(F.col("n_events").desc(), "source", "burst_id")
w_source_n_events = Window.partitionBy("source", "n_events")

valid = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("search", "playlist"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
    )
)

with_prev = valid.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_source_running))

session_summary = (
    sessionized
    .groupBy("source", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    session_summary
    .withColumn("next3_burst_id", F.lead("burst_id", 3).over(w_burst_id_by_n_events))
    .withColumn("rows_in_source_n_events", F.count(F.lit(1)).over(w_source_n_events))
)

result = (
    windowed
    .select(
        "source",
        "burst_id",
        "session_start",
        "session_end",
        "duration_min",
        "next3_burst_id",
        "rows_in_source_n_events",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("session_start")).alias("days_since_session_start"),
        F.coalesce(F.col("next3_burst_id"), F.lit(0)).alias("next3_burst_id_filled"),
    )
    .orderBy("rows_in_source_n_events", "session_start")
)
