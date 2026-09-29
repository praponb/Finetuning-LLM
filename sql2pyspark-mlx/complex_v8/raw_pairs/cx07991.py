from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_plays
    .filter(F.col("device") == "speaker")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

with_prev = cleaned.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_track_id_running))

per_session = (
    sessionized
    .groupBy("track_id", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    per_session
    .filter(F.col("events") <= 4)
    .groupBy("events", "session_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("session_id").alias("max_session_id"),
        F.max("session_start").alias("last_session_start"),
        F.round(F.stddev("duration_min"), 2).alias("stddev_duration_min"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    per_group
    .groupingSets(
        [["session_id", "n_rows"], ["session_id"], []],
        "session_id", "n_rows",
    )
    .agg(
        F.grouping("session_id").alias("is_total_session_id"),
        F.min("last_session_start").alias("first_last_session_start"),
    )
    .orderBy(F.col("session_id").asc_nulls_last(), F.col("n_rows").asc_nulls_last())
)
