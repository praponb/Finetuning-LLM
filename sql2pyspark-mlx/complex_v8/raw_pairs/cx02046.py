from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_session_no_by_session = Window.partitionBy("source", "session_no").orderBy("session_end", "source", "session_no")
w_session_no = Window.partitionBy("session_no")
w_source_session_no_by_session_2 = Window.partitionBy("source", "session_no").orderBy("session_end", "source", "session_no").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
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
    )
)

with_prev = prepared.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_source_running))

session_summary = (
    sessionized
    .groupBy("source", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    session_summary
    .select(
        "source",
        "session_no",
        "events",
        F.ntile(10).over(w_source_session_no_by_session).alias("session_end_decile"),
        F.min("events").over(w_session_no).alias("session_no_min_events"),
        (F.col("session_no") - F.lag("session_no").over(w_source_session_no_by_session)).alias("session_no_change"),
        F.first("duration_min").over(w_source_session_no_by_session_2).alias("first_duration_min"),
        F.round(F.col("session_no") / F.nullif(F.sum("session_no").over(w_session_no), F.lit(0)), 4).alias("session_no_share_of_session_no"),
    )
)

result = (
    with_windows
    .select(
        "source",
        "events",
        "session_end_decile",
        "session_no_min_events",
        "session_no_change",
        "first_duration_min",
        "session_no_share_of_session_no",
        F.when(F.col("session_no") >= 4, "A").when(F.col("session_no") >= 3, "B").otherwise("C").alias("session_no_tier"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
    .orderBy("events")
)
