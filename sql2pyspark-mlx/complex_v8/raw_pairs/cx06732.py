from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.col("bpm") <= 143)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
    )
)

joined_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

exploded_detail = (
    joined_artist.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.moods",
        "s.artist_genre",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    exploded_detail
    .cube("play_source", "artist_id", "artist_genre")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("play_device")).alias("play_device_list"),
    )
    .select(
        F.coalesce(F.col("play_source"), F.lit("ALL")).alias("play_source"),
        "artist_id",
        F.coalesce(F.col("artist_genre"), F.lit("ALL")).alias("artist_genre"),
        "grouping_level",
        "play_device_list",
    )
    .orderBy(
        F.col("play_source").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
        F.col("artist_genre").asc_nulls_last(),
    )
)
