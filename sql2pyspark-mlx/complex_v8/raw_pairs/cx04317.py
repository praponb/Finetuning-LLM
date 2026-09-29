from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ms_played_tier = Window.partitionBy("ms_played_tier").orderBy("played_at", "id")
w_ms_played_tier_running = Window.partitionBy("ms_played_tier").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end_prior_2 = Window.orderBy(F.col("session_end").desc(), "ms_played_tier", "session_id").rowsBetween(-2, -1)
w_all_by_session_end_running = Window.orderBy(F.col("session_end").desc(), "ms_played_tier", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_start_month = Window.partitionBy(F.date_trunc("month", "session_start"))

base = (
    df_plays
    .filter(F.col("ms_played") >= 98052)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 241585, "A").when(F.col("ms_played") >= 107912, "B").otherwise("C").alias("ms_played_tier"),
    )
)

with_prev = base.withColumn("prev_played_at", F.lag("played_at").over(w_ms_played_tier))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_ms_played_tier_running))

per_session = (
    sessionized
    .groupBy("ms_played_tier", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    per_session
    .select(
        "ms_played_tier",
        "session_id",
        "session_start",
        "events",
        "session_ms_played",
        "duration_min",
        F.round(F.avg("duration_min").over(w_all_by_session_end_prior_2), 2).alias("duration_min_avg_2_prior"),
        F.min("events").over(w_all_by_session_end_running).alias("min_events_so_far"),
        F.round(F.col("session_id") / F.nullif(F.sum("session_id").over(w_session_start_month), F.lit(0)), 4).alias("session_id_share_of_session_start_month"),
    )
)

without_matches = (
    ranked
    .crossJoin(ranked.agg(F.max("min_events_so_far").alias("overall_max_min_events_so_far")))
    .filter(F.col("min_events_so_far") < F.col("overall_max_min_events_so_far"))
    .drop("overall_max_min_events_so_far")
)

result = (
    without_matches
    .select(
        "ms_played_tier",
        "session_id",
        "session_start",
        "events",
        "session_ms_played",
        "duration_min",
        "duration_min_avg_2_prior",
        "min_events_so_far",
        "session_id_share_of_session_start_month",
        F.when(F.col("duration_min") >= 204.0, "large").when(F.col("duration_min") >= 48.0, "medium").otherwise("small").alias("duration_min_tier"),
        F.coalesce(F.col("ms_played_tier"), F.lit("UNKNOWN")).alias("ms_played_tier_or_unknown"),
    )
    .orderBy(F.col("session_id").asc(), "ms_played_tier")
    .limit(20)
)
