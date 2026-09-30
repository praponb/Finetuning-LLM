from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_count = Window.orderBy("event_count", "source", "session_id")
w_session_start_month = Window.partitionBy(F.month("session_start"))
w_source_event_count_by_sessio = Window.partitionBy("source", "event_count").orderBy("session_end", "source", "session_id")
w_all_by_event_count_full = Window.orderBy("event_count", "source", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_plays = (
    df_plays
    .filter(F.col("device").isin("speaker", "tv"))
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

with_prev = cleaned_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_source_running))

per_session = (
    sessionized
    .groupBy("source", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    per_session
    .select(
        "source",
        "session_id",
        "session_start",
        "session_ms_played",
        "duration_min",
        F.row_number().over(w_all_by_event_count).alias("event_count_seq"),
        F.count(F.lit(1)).over(w_session_start_month).alias("rows_in_session_start_month"),
        (F.col("session_ms_played") - F.lag("session_ms_played").over(w_source_event_count_by_sessio)).alias("session_ms_played_change"),
        F.first("session_start").over(w_all_by_event_count_full).alias("first_session_start"),
    )
)

result = (
    with_trends
    .select(
        "source",
        "session_id",
        "session_start",
        "session_ms_played",
        "duration_min",
        "event_count_seq",
        "session_ms_played_change",
        "first_session_start",
        F.round(F.col("event_count_seq") / F.nullif(F.col("session_ms_played_change"), F.lit(0)), 3).alias("event_count_seq_per_session_ms_played_change"),
    )
    .orderBy(F.col("session_ms_played").desc(), "source", "session_id")
    .limit(10)
)
