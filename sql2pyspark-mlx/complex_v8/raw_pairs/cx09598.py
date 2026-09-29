from pyspark.sql import functions as F

scoped = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

with_details = (
    scoped.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .filter(F.col("t.duration_sec") >= 142)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    with_details
    .rollup("track_id", "is_skipped", "device")
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("n_rows"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("n_not_is_skipped"),
    )
    .select(
        "track_id",
        "is_skipped",
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "gid",
        "n_rows",
        "n_not_is_skipped",
    )
)
