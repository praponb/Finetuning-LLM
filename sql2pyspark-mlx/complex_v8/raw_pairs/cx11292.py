from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_avg_ms_played_full = Window.orderBy("avg_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id = Window.partitionBy("track_id")
w_listener_id_is_skipped_by_ms = Window.partitionBy("listener_id", "is_skipped").orderBy("ms_played_minus_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_avg_ms_played = Window.orderBy("avg_ms_played", "id")

valid_plays = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")) & (F.col("device") != "desktop"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

source_stats = (
    valid_plays
    .groupBy("source")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

outliers = (
    valid_plays.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

with_windows = (
    outliers
    .select(
        "id",
        "listener_id",
        "ms_played",
        "is_skipped",
        "avg_ms_played",
        F.first("ms_played_minus_avg").over(w_all_by_avg_ms_played_full).alias("first_ms_played_minus_avg"),
        F.size(F.collect_set("source").over(w_track_id)).alias("distinct_source_in_track_id"),
        F.first("ms_played_minus_avg", ignorenulls=True).over(w_listener_id_is_skipped_by_ms).alias("first_known_ms_played_minus_avg"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.lead("ms_played_minus_avg", 2).over(w_all_by_avg_ms_played).alias("next2_ms_played_minus_avg"),
        F.row_number().over(w_all_by_avg_ms_played).alias("avg_ms_played_seq"),
    )
)

without_matches = (
    with_windows.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.plan").isNotNull()), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "first_ms_played_minus_avg",
        "distinct_source_in_track_id",
        "first_known_ms_played_minus_avg",
        "rows_in_track_id",
        "avg_ms_played_seq",
    )
    .orderBy("id", F.col("first_ms_played_minus_avg").desc())
)
