from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_events = Window.orderBy("n_events", "source", "burst_id")
w_all_by_session_end_full = Window.orderBy(F.col("session_end").desc(), "source", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_burst_id = Window.partitionBy("source", "burst_id")

prepared_plays = (
    df_plays
    .filter(F.col("source").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
    )
)

with_prev = prepared_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_source_running))

per_session = (
    sessionized
    .groupBy("source", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    per_session
    .select(
        "source",
        "burst_id",
        "session_start",
        "session_ms_played",
        "duration_min",
        (F.col("n_events") - F.lag("n_events").over(w_all_by_n_events)).alias("n_events_change"),
        F.rank().over(w_all_by_n_events).alias("n_events_rank"),
        F.last("session_start").over(w_all_by_session_end_full).alias("last_session_start"),
        F.round(F.avg("burst_id").over(w_source_burst_id), 2).alias("source_burst_id_avg_burst_id"),
    )
)

tagged = (
    windowed
    .select(
        "source",
        "burst_id",
        "session_start",
        "session_ms_played",
        "duration_min",
        "n_events_change",
        "last_session_start",
        "source_burst_id_avg_burst_id",
        F.when(F.col("n_events_rank") >= 8, "A").when(F.col("n_events_rank") >= 4, "B").otherwise("C").alias("n_events_rank_tier"),
        F.date_trunc("quarter", F.col("last_session_start")).alias("last_session_start_quarter"),
    )
)

result = (
    tagged
    .select(
        "source",
        "burst_id",
        "session_start",
        "session_ms_played",
        "duration_min",
        "n_events_change",
        "last_session_start",
        "source_burst_id_avg_burst_id",
        F.concat(F.col("n_events_rank_tier"), F.lit("#"), F.col("session_ms_played").cast("string")).alias("n_events_rank_tier_ref"),
        F.concat_ws(" / ", F.col("source"), F.col("n_events_rank_tier")).alias("source_n_events_rank_tier_label"),
    )
    .orderBy(
        F.col("source_n_events_rank_tier_label").asc(),
        F.col("n_events_rank_tier_ref").desc(),
        "source",
        "burst_id",
    )
    .limit(10)
)
