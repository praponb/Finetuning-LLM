from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source").orderBy("played_at", "id")
w_source_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_source = Window.partitionBy("track_id", "source")
w_ms_played_tier_played_at_mon = Window.partitionBy("ms_played_tier", F.month("played_at")).orderBy("starts_session", "id")

prepared_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 223262, "A").when(F.col("ms_played") >= 157900, "B").otherwise("C").alias("ms_played_tier"),
    )
)

with_prev = prepared_plays.withColumn("prev_played_at", F.lag("played_at").over(w_source))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_source_running))

with_windows = (
    sessionized
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "ms_played_tier",
        "prev_played_at",
        F.size(F.collect_set("ms_played_tier").over(w_track_id_source)).alias("distinct_ms_played_tier_in_track_id_source"),
        F.ntile(10).over(w_ms_played_tier_played_at_mon).alias("starts_session_decile"),
        F.row_number().over(w_ms_played_tier_played_at_mon).alias("rn"),
        F.max("ms_played").over(w_track_id_source).alias("track_id_source_max_ms_played"),
    )
)

annotated = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "prev_played_at",
        "distinct_ms_played_tier_in_track_id_source",
        "starts_session_decile",
        "rn",
        "track_id_source_max_ms_played",
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
        F.date_trunc("week", F.col("played_at")).alias("played_at_week"),
    )
)

result = (
    annotated
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "prev_played_at",
        "distinct_ms_played_tier_in_track_id_source",
        "starts_session_decile",
        "rn",
        "track_id_source_max_ms_played",
        "played_at_week",
        F.dayofweek(F.col("prev_played_at")).alias("prev_played_at_dayofweek"),
        F.abs(F.col("rn") - 7).alias("rn_distance"),
    )
)
