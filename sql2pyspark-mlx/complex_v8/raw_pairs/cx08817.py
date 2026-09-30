from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.col("bpm") >= 85) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

joined_artist = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.length(F.trim(F.col("a.artist_name"))) >= 8), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        "a.artist_name",
    )
)

qualified = (
    joined_artist.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.source") == "playlist") & (F.col("p.listener_id").isNotNull()), "left_anti")
    .filter(F.col("artist_id") >= 4)
)

flattened = (
    qualified
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "artist_name",
        F.explode_outer("moods").alias("mood"),
    )
)

result = (
    flattened
    .cube("artist_id", "mood", "is_explicit")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)
