from pyspark.sql import functions as F

filtered = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
    )
)

with_activity = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id") <= 6), "left_semi")
)

joined = (
    with_activity.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    joined
    .groupingSets(
        [["artist_genre", "artist_country", "artist_id"], ["artist_genre"], ["artist_country"]],
        "artist_genre", "artist_country", "artist_id",
    )
    .agg(F.countDistinct("artist_id").alias("distinct_artist_ids"))
    .select(
        F.coalesce(F.col("artist_genre"), F.lit("ALL")).alias("artist_genre"),
        F.coalesce(F.col("artist_country"), F.lit("ALL")).alias("artist_country"),
        "artist_id",
        "distinct_artist_ids",
    )
)
