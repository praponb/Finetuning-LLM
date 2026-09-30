from pyspark.sql import functions as F

recent_plays = (
    df_plays
    .filter(F.col("ms_played") > 95960)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

combined_details = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .filter(F.col("t.artist_id").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_details_artist = (
    combined_details.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.track_artist_id"), "left")
    .select("s.*", F.col("a.is_verified").alias("artist_is_verified"))
)

result = (
    with_details_artist
    .groupingSets(
        [["track_artist_id", "is_skipped", "listener_id"], ["track_artist_id"]],
        "track_artist_id", "is_skipped", "listener_id",
    )
    .agg(
        F.grouping("track_artist_id").alias("is_total_track_artist_id"),
        F.count("*").alias("cnt"),
        F.sum("artist_debut_year").alias("total_artist_debut_year"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
    )
)
