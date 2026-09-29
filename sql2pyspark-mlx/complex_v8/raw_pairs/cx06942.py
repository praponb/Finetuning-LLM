from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("ms_played") > 176302)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        (F.col("played_at") + F.expr("INTERVAL 15 MINUTES")).alias("played_at_plus_15m"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

filtered = (
    base.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") != "tv"), "left_anti")
)

enriched = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.device_clean",
        "s.played_at_plus_15m",
        "s.source_or_unknown",
        F.col("t.title").alias("track_title"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

stats = (
    enriched
    .groupBy("listener_id")
    .agg(
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("track_title"))).like("G%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_track_title_like"),
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(~F.col("is_skipped"), 1)) / F.count("*"), 1).alias("pct_not_is_skipped"),
    )
)

with_details_listener = (
    stats.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.plan") == "free"), "inner")
    .select(
        "s.*",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
        F.col("l.country").alias("listener_country"),
    )
)

screened = (
    with_details_listener.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.source") == "playlist"), "left_semi")
    .filter(F.col("pct_not_is_skipped") <= 90.0)
)

exploded = (
    screened
    .select(
        "listener_id",
        "pct_upper_trim_track_title_like",
        "cnt",
        "pct_not_is_skipped",
        "listener_username",
        "listener_country",
        F.explode("listener_interests").alias("listener_interest"),
    )
)

result = (
    exploded
    .select(
        "listener_id",
        "pct_not_is_skipped",
        "listener_username",
        "listener_interest",
        F.substring(F.trim(F.col("listener_interest")), 1, 2).alias("listener_interest_prefix"),
        (F.col("cnt") + F.col("pct_not_is_skipped")).alias("cnt_plus_pct_not_is_skipped"),
    )
    .orderBy("cnt_plus_pct_not_is_skipped", "listener_interest")
)
