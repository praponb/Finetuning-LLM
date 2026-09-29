from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_plays = (
    df_plays
    .filter(F.col("source") == "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

with_prev = cleaned_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_source_running))

per_session = (
    sessionized
    .groupBy("source", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    per_session
    .filter(F.col("source") != "album")
    .select("*", F.initcap(F.trim(F.col("source"))).alias("source_clean"))
)

annotated = (
    labeled
    .filter(F.col("session_start") >= F.lit("2025-01-17 09:00:00").cast("timestamp"))
    .select(
        "source",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "source_clean",
        F.when(F.col("event_count") >= 4, "HIGH").when(F.col("event_count") >= 2, "MEDIUM").otherwise("LOW").alias("event_count_tier"),
        F.coalesce(F.col("session_id"), F.lit(0)).alias("session_id_filled"),
    )
)

result = (
    annotated
    .select(
        "source",
        "session_end",
        "source_clean",
        "event_count_tier",
        "session_id_filled",
    )
    .orderBy(F.col("event_count_tier").desc_nulls_first())
)
