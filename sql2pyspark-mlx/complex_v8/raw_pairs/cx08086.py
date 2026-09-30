from pyspark.sql import functions as F

filtered = (
    df_plays
    .filter((F.col("device") == "phone") | (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

joined_listener = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.country").isNotNull())
    .select("s.*", F.col("l.country").alias("listener_country"))
)

combined_details_track = (
    joined_listener.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.bpm") <= 147)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.played_at_date",
        "s.listener_country",
        F.col("t.title").alias("track_title"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    combined_details_track
    .select("id", "ms_played", "track_bpm")
    .unpivot(
        ids=["id"],
        values=["ms_played", "track_bpm"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
