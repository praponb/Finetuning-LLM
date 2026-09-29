from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_listener_id_running = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .filter(~F.col("device").isin("tv", "phone"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

with_prev = prepared_plays.withColumn("prev_played_at", F.lag("played_at").over(w_listener_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_listener_id_running))

session_summary = (
    sessionized
    .groupBy("listener_id", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

screened = (
    session_summary.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.hour(F.col("p.played_at")).between(7, 11)), "left_semi")
)

labeled = (
    screened
    .select(
        "listener_id",
        "session_no",
        "session_start",
        "session_end",
        "n_events",
        "session_ms_played",
        F.round(F.col("duration_min") * 1.5, 2).alias("duration_min_adj"),
        F.round(F.col("n_events") / F.nullif(F.col("session_no"), F.lit(0)), 3).alias("n_events_per_session_no"),
        F.date_trunc("quarter", F.col("session_start")).alias("session_start_quarter"),
    )
)

result = (
    labeled
    .select(
        "listener_id",
        "session_no",
        "session_start",
        "session_end",
        "session_ms_played",
        "duration_min_adj",
        "session_start_quarter",
        F.to_date(F.col("session_start_quarter")).alias("session_start_quarter_date"),
        F.round(F.col("n_events_per_session_no") / F.nullif(F.col("n_events"), F.lit(0)), 3).alias("n_events_per_session_no_per_n_events"),
    )
    .orderBy(
        F.col("listener_id").desc_nulls_first(),
        F.col("session_ms_played").desc(),
        "session_no",
    )
    .limit(5)
)
