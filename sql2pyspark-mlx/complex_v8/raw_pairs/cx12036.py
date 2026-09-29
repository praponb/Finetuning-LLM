from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("bpm").desc(), F.col("id").desc())

recent = (
    df_tracks
    .filter((F.col("bpm") <= 127) & (F.col("release_date") >= F.lit("2024-12-03").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

with_details_artist = (
    recent.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year").between(1996, 2015)), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.days_since_release_date",
        "s.release_date_plus_7d",
        "s.duration_sec_filled",
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_parent_artist = (
    with_details_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

latest = (
    with_parent_artist
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupingSets(
        [["artist_genre", "artist_id"], ["artist_genre"], ["artist_id"], []],
        "artist_genre", "artist_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("row_count"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
        F.round(F.avg("days_since_release_date"), 2).alias("avg_days_since_release_date"),
    )
    .select(
        F.coalesce(F.col("artist_genre"), F.lit("ALL")).alias("artist_genre"),
        "artist_id",
        "grouping_level",
        "row_count",
        "release_date_span_days",
        "avg_days_since_release_date",
    )
    .orderBy(
        F.col("artist_genre").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
