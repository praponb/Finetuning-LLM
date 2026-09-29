from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_all_by_ms_played_change_fill = Window.orderBy(F.col("ms_played_change_filled").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_played_at_plus_1 = Window.partitionBy("listener_id", F.month("played_at_plus_15m"))
w_all_by_ms_played_change_fill_2 = Window.orderBy(F.col("ms_played_change_filled").asc_nulls_last(), "id")

filtered = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) | (F.col("source") == "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.when(F.col("source").isin("radio", "playlist"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
    )
)

without_matches = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.month(F.col("p.played_at")).isin(2, 3)), "left_semi")
)

labeled = (
    without_matches
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "source_or_unknown",
        "source_group",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.coalesce(F.col("source_or_unknown"), F.lit("UNKNOWN")).alias("source_or_unknown_or_unknown"),
        (F.col("played_at") + F.expr("INTERVAL 15 MINUTES")).alias("played_at_plus_15m"),
    )
)

combined_details_track = (
    labeled.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.source_or_unknown",
        "s.days_since_played_at",
        "s.played_at_plus_15m",
        F.col("t.release_date").alias("track_release_date"),
        F.col("l.username").alias("listener_username"),
    )
)

windowed = (
    combined_details_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        "days_since_played_at",
        "played_at_plus_15m",
        "track_release_date",
        "listener_username",
        F.lag("ms_played", 1, 0).over(w_all_by_ms_played).alias("prev_ms_played"),
        F.lag("days_since_played_at", 3, 0).over(w_all_by_ms_played).alias("prev3_days_since_played_at"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_ms_played)).alias("ms_played_change"),
        F.round(F.col("days_since_played_at") - F.avg("days_since_played_at").over(w_listener_id_track_id), 2).alias("days_since_played_at_vs_listener_id_track_id_avg"),
        F.round((F.unix_timestamp("played_at_plus_15m") - F.unix_timestamp(F.lag("played_at_plus_15m").over(w_all_by_ms_played))) / 60, 1).alias("minutes_since_prev_played_at_plus_15m"),
    )
)

enriched = (
    windowed
    .select(
        "id",
        "listener_id",
        "ms_played",
        "source",
        "played_at_plus_15m",
        "listener_username",
        "days_since_played_at_vs_listener_id_track_id_avg",
        "minutes_since_prev_played_at_plus_15m",
        F.concat(F.col("listener_username"), F.lit("#"), F.col("prev_ms_played").cast("string")).alias("listener_username_ref"),
        F.date_add(F.col("track_release_date"), 90).alias("track_release_date_plus_90d"),
        F.coalesce(F.col("ms_played_change"), F.lit(0)).alias("ms_played_change_filled"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.first("ms_played_change_filled", ignorenulls=True).over(w_all_by_ms_played_change_fill).alias("first_known_ms_played_change_filled"),
        F.max("ms_played").over(w_listener_id_played_at_plus_1).alias("listener_id_played_at_plus_15m_month_max_ms_play"),
        F.lag("ms_played", 2).over(w_all_by_ms_played_change_fill_2).alias("prev2_ms_played"),
    )
)

result = (
    metrics
    .select(
        "id",
        "listener_id",
        "ms_played",
        "source",
        "played_at_plus_15m",
        "listener_username",
        "days_since_played_at_vs_listener_id_track_id_avg",
        "minutes_since_prev_played_at_plus_15m",
        "listener_username_ref",
        "track_release_date_plus_90d",
        "ms_played_change_filled",
        "first_known_ms_played_change_filled",
        "listener_id_played_at_plus_15m_month_max_ms_play",
        "prev2_ms_played",
        F.when(F.col("source").isin("playlist", "album"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
    .orderBy(
        F.col("first_known_ms_played_change_filled").desc_nulls_first(),
        F.col("source_or_unknown").asc(),
    )
)
