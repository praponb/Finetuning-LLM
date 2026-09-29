from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_flag_artist_id = Window.partitionBy("is_explicit_flag", "artist_id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc())

recent_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

with_parent_artist = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_parent_artist_2 = (
    with_parent_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") < 2015)
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.is_explicit_flag",
        "s.artist_debut_year",
        F.col("a.debut_year").alias("artist_debut_year_2"),
        F.col("a.country").alias("artist_country"),
    )
)

metrics = (
    with_parent_artist_2
    .withColumn("distinct_artist_country_in_is_explicit_flag_arti", F.size(F.collect_set("artist_country").over(w_is_explicit_flag_artist_id)))
    .withColumn("release_date_percent_rank", F.round(F.percent_rank().over(w_is_explicit_by_release_date), 3))
)

result = (
    metrics
    .groupBy("artist_id", "is_explicit_flag")
    .pivot("artist_country", ["KR", "JP"])
    .agg(F.sum("distinct_artist_country_in_is_explicit_flag_arti"))
)
