from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.artist_name").isNotNull())
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.moods",
        "s.title_prefix",
        "s.bpm_filled",
        "a.artist_name",
    )
)

with_activity = (
    joined.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

combined_details = (
    with_activity.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm_filled",
        "s.artist_name",
        F.col("a.artist_name").alias("artist_artist_name"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "bpm_filled",
        "artist_name",
        F.concat_ws(" / ", F.col("artist_artist_name"), F.col("artist_name")).alias("artist_artist_name_artist_name_label"),
    )
)
