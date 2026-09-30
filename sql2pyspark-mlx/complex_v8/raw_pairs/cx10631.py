from pyspark.sql import functions as F

valid = (
    df_plays
    .filter((F.to_date(F.col("played_at")) < F.lit("2025-03-02").cast("date")) & (F.col("ms_played") > 129336))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

joined = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        F.col("t.title").alias("track_title"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_activity = (
    joined.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device").isin("speaker", "tv", "car")) & (~F.col("p.is_skipped")), "left_semi")
)

with_details = (
    with_activity.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.lower(F.col("l.username")).like("%.s%"))
    .select(
        "s.*",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

without_matches = (
    with_details.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("listener_signup_date").between(F.lit("2024-08-02").cast("date"), F.lit("2025-07-08").cast("date")))
)

detail_rows_plays = (
    without_matches.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.track_title",
        "s.artist_debut_year",
        "s.listener_signup_date",
        "s.listener_username",
        "s.track_release_date",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    detail_rows_plays
    .select(
        "id",
        "play_id",
        F.col("artist_debut_year").alias("m_artist_debut_year"),
        F.col("ms_played").alias("ms_played_v"),
        F.col("play_ms_played").alias("m_play_ms_played"),
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["m_artist_debut_year", "ms_played_v", "m_play_ms_played"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
