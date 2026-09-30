from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(1, 2))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

with_prev = prepared.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_listener_id_running))

sessions = (
    sessionized
    .groupBy("listener_id", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    sessions
    .filter(F.hour(F.col("session_start")).between(7, 13))
    .select(
        "listener_id",
        "session_no",
        "session_start",
        "session_end",
        "session_ms_played",
        "duration_min",
        F.date_format(F.col("session_start"), "MMM yyyy").alias("session_start_label"),
        F.date_trunc("year", F.col("session_end")).alias("session_end_year"),
    )
)

result = (
    labeled
    .filter(F.col("session_ms_played") <= 1440500)
    .select(
        "listener_id",
        "session_no",
        "session_start",
        "session_end",
        "session_ms_played",
        "session_start_label",
        "session_end_year",
        F.when(F.col("session_no") >= 5, "large").when(F.col("session_no") >= 2, "medium").otherwise("small").alias("session_no_tier"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("session_end")).alias("days_since_session_end"),
    )
    .orderBy(F.col("session_no").desc(), F.col("listener_id").asc_nulls_last())
)
