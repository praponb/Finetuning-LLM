from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter((F.col("duration_sec") < 392) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
    )
)

tagged = scoped.select("*", F.abs(F.col("duration_sec") - 352).alias("duration_sec_distance"))

joined_artist = (
    tagged.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.moods",
        "s.duration_sec_distance",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device").isNotNull(), 1)).alias("play_device_count"),
        F.count(F.when(F.col("source") != "radio", 1)).alias("play_source_count"),
        F.count("*").alias("play_count"),
    )
)

with_stats = (
    joined_artist.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    with_stats
    .groupBy("artist_id")
    .pivot("artist_country", ["TH", "KR", "US"])
    .agg(F.min("artist_debut_year"))
)
