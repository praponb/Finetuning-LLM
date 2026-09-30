from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_country").orderBy(F.col("bpm").desc(), F.col("id").desc())

recent = (
    df_tracks
    .filter((F.col("bpm") >= 96) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.title_slug",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

latest = (
    with_details
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(~F.col("source").isin("search", "playlist"), 1)).alias("play_source_count"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

profiled = (
    latest.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        "p.avg_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["play_count", "artist_is_verified", "artist_country"], ["play_count"], ["artist_is_verified"], []],
        "play_count", "artist_is_verified", "artist_country",
    )
    .agg(F.countDistinct("artist_genre").alias("distinct_artist_genres"))
    .orderBy(
        F.col("play_count").asc_nulls_last(),
        F.col("artist_is_verified").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
    )
)
