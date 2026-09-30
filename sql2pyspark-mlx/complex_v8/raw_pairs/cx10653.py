from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_country_source_by_p = Window.partitionBy("listener_country", "source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_listener_signup_date = Window.orderBy("listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_listener_signup_date_2 = Window.orderBy("listener_signup_date", "id")
w_all_by_listener_signup_date_3 = Window.orderBy("listener_signup_date")

base_plays = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")) & (F.col("track_id").isNotNull()))
)

joined = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.country").alias("listener_country"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.nth_value("played_at", 2).over(w_listener_country_source_by_p).alias("second_played_at"),
        F.nth_value("listener_signup_date", 2).over(w_all_by_listener_signup_date).alias("second_listener_signup_date"),
        F.lead("played_at", 3).over(w_all_by_listener_signup_date_2).alias("next3_played_at"),
        F.last("played_at").over(w_listener_country_source_by_p).alias("last_played_at"),
        F.round(F.cume_dist().over(w_all_by_listener_signup_date_3), 3).alias("listener_signup_date_cume_dist"),
    )
)

device_stats = (
    with_windows
    .groupBy("device")
    .agg(F.avg("listener_signup_date_cume_dist").alias("avg_listener_signup_date_cume_dist"))
)

outliers = (
    with_windows.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.listener_signup_date_cume_dist") > F.col("g.avg_listener_signup_date_cume_dist"))
    .select(
        "s.*",
        "g.avg_listener_signup_date_cume_dist",
        F.round(F.col("s.listener_signup_date_cume_dist") - F.col("g.avg_listener_signup_date_cume_dist"), 2).alias("listener_signup_date_cume_dist_minus_avg"),
    )
)

joined_listener = (
    outliers.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.*",
        F.col("l.country").alias("listener_country_2"),
        F.col("l.interests").alias("listener_interests"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

with_parent_listener = (
    joined_listener.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year") < 1990), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.listener_signup_date",
        "s.listener_country",
        "s.track_is_explicit",
        "s.second_listener_signup_date",
        "s.listener_signup_date_cume_dist",
        "s.avg_listener_signup_date_cume_dist",
        "s.listener_signup_date_cume_dist_minus_avg",
        "s.listener_country_2",
        "s.listener_interests",
        "s.track_bpm",
        "s.track_duration_sec",
        F.col("l.signup_date").alias("listener_signup_date_2"),
    )
)

result = (
    with_parent_listener
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "played_at",
        "is_skipped",
        "source",
        "listener_signup_date",
        "track_is_explicit",
        "second_listener_signup_date",
        "avg_listener_signup_date_cume_dist",
        "listener_interests",
        "listener_signup_date_2",
        (F.col("track_duration_sec") - F.col("track_bpm")).alias("track_duration_sec_minus_track_bpm"),
        F.abs(F.col("track_duration_sec") - 209).alias("track_duration_sec_distance"),
    )
)
