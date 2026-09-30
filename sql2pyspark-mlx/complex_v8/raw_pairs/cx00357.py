from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-12 09:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.when(F.col("source").isin("album", "radio"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
    )
)

joined_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.size(F.col("t.moods")) >= 2), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source_group",
        F.col("t.moods").alias("track_moods"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

screened = (
    joined_track.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.played_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")) & (F.col("p.ms_played") >= 95960), "left_anti")
)

scored = (
    screened
    .select(
        "*",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

result = (
    scored
    .groupingSets(
        [["track_id", "is_skipped"], ["track_id"], []],
        "track_id", "is_skipped",
    )
    .agg(
        F.grouping("track_id").alias("is_total_track_id"),
        F.percentile_approx("is_skipped_flag", 0.5).alias("median_is_skipped_flag"),
        F.countDistinct("artist_genre").alias("distinct_artist_genres"),
    )
)
