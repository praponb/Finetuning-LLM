from pyspark.sql import functions as F

valid = (
    df_plays
    .filter((F.col("source") == "album") | (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
    )
)

joined = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.is_explicit"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.source_clean",
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

filtered = (
    joined.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source").isin("radio", "search", "album")) & (F.col("p.listener_id") >= 5), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        "track_is_explicit",
        (F.col("played_at") + F.expr("INTERVAL 30 MINUTES")).alias("played_at_plus_30m"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
    .orderBy(F.col("played_at").asc_nulls_last(), "id")
    .limit(10)
)
