from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter(F.col("ms_played") < 324437)
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

with_parent = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.title").alias("track_title"),
    )
)

labeled = (
    with_parent
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "listener_signup_date",
        "track_bpm",
        (F.col("track_bpm") + F.col("ms_played")).alias("track_bpm_plus_ms_played"),
    )
)

enriched = (
    labeled
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "listener_signup_date",
        "track_bpm",
        "track_bpm_plus_ms_played",
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.abs(F.col("track_bpm_plus_ms_played") - 260138).alias("track_bpm_plus_ms_played_distance"),
    )
)

enriched_2 = (
    enriched
    .filter(F.col("track_bpm") < 127)
    .select(
        "*",
        F.date_trunc("month", F.col("listener_signup_date")).alias("listener_signup_date_month"),
        F.date_format(F.col("played_at_plus_1d"), "yyyy-MM").alias("played_at_plus_1d_label"),
    )
)

rollup_base = (
    enriched_2
    .groupBy(
        F.date_format("listener_signup_date", "yyyy-MM").alias("listener_signup_date_ym"),
        F.to_date("played_at_plus_1d").alias("played_at_plus_1d_day"),
        F.to_date("listener_signup_date_month").alias("listener_signup_date_month_day"),
    )
    .agg(
        F.max("track_bpm_plus_ms_played").alias("max_track_bpm_plus_ms_played"),
        F.max("ms_played_filled").alias("max_ms_played_filled"),
    )
)

derived = (
    rollup_base
    .filter(F.col("played_at_plus_1d_day") < F.lit("2025-03-01").cast("date"))
    .select(
        "listener_signup_date_ym",
        "played_at_plus_1d_day",
        "listener_signup_date_month_day",
        F.trunc(F.col("played_at_plus_1d_day"), "MM").alias("played_at_plus_1d_day_month_start"),
        F.substring(F.trim(F.col("listener_signup_date_ym")), 1, 3).alias("listener_signup_date_ym_prefix"),
        F.when(F.col("max_ms_played_filled") >= 242000, "A").when(F.col("max_ms_played_filled") >= 123500, "B").otherwise("C").alias("max_ms_played_filled_tier"),
    )
)

result = (
    derived
    .select(
        "listener_signup_date_ym",
        "played_at_plus_1d_day",
        "listener_signup_date_month_day",
        "played_at_plus_1d_day_month_start",
        "max_ms_played_filled_tier",
        F.date_add(F.col("played_at_plus_1d_day"), 30).alias("played_at_plus_1d_day_plus_30d"),
        F.add_months(F.col("played_at_plus_1d_day"), 3).alias("played_at_plus_1d_day_plus_3m"),
    )
)
