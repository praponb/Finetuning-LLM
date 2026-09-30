from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_source_running))

session_summary = (
    sessionized
    .groupBy("source", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    session_summary
    .filter(F.month(F.col("session_end")).isin(2, 3))
    .select(
        "*",
        F.concat(F.col("source"), F.lit("#"), F.col("session_ms_played").cast("string")).alias("source_ref"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_end")).alias("days_since_session_end"),
        F.abs(F.col("n_events") - 3).alias("n_events_distance"),
    )
)

enriched = (
    annotated
    .filter(F.col("source_ref").isNotNull())
    .select(
        "*",
        F.length(F.trim(F.col("source_ref"))).alias("source_ref_len"),
        F.date_format(F.col("session_end"), "MMM yyyy").alias("session_end_label"),
    )
)

result = (
    enriched
    .select(
        "source",
        "session_no",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        "source_ref",
        (F.floor(F.col("source_ref_len") / 2) * 2).alias("source_ref_len_bucket"),
        F.concat_ws(" / ", F.col("source_ref"), F.col("source")).alias("source_ref_source_label"),
    )
    .orderBy(
        F.col("session_start").desc(),
        F.col("source_ref_len_bucket").desc(),
        "source",
        "session_no",
    )
    .limit(20)
)
