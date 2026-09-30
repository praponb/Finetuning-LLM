from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

labeled = (
    scoped_tracks
    .withColumn("is_explicit_label", F.when(F.col("is_explicit"), "yes").otherwise("no"))
)

enriched = (
    labeled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.moods",
        "s.title_prefix",
        "s.is_explicit_label",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

tagged = (
    enriched
    .select(
        "*",
        F.concat(F.col("title_prefix"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_prefix_ref"),
    )
)

with_parent_artist = (
    tagged.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

with_activity = (
    with_parent_artist.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.device").isin("phone", "desktop")) & (F.col("p.source") == "playlist"), "left_anti")
)

joined = (
    with_activity.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.moods",
        "s.title_prefix",
        "s.is_explicit_label",
        "s.artist_is_verified",
        "s.artist_name",
        "s.artist_genre",
        "s.artist_country",
        F.col("a.is_verified").alias("artist_is_verified_2"),
    )
)

result = (
    joined
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        "is_explicit_label",
        "artist_is_verified",
        "artist_genre",
        "artist_is_verified_2",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
    .orderBy(F.col("artist_genre").asc_nulls_last())
)
