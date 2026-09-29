from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())

base = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "party")) & (F.col("artist_id") <= 3))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

scored = (
    base
    .filter(F.col("least_bpm_duration_sec") > 204)
    .withColumn("days_release_date_plus_7d_to_release_date", F.datediff(F.col("release_date"), F.col("release_date_plus_7d")))
)

with_parent_artist = (
    scored.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

latest = (
    with_parent_artist
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .cube("is_explicit", "artist_genre", "artist_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_genre").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
