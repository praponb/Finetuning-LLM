from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_artist_debut = Window.partitionBy("is_explicit").orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_artist_debut_2 = Window.partitionBy("is_explicit").orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tracks
    .filter(F.col("duration_sec").between(165, 409))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

joined_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.moods",
        "s.title_clean",
        "s.release_date_year",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

tagged = (
    joined_artist
    .filter(~F.array_contains(F.col("moods"), "sad"))
    .withColumn("title_clean_artist_country_label", F.concat_ws(": ", F.col("title_clean"), F.col("artist_country")))
)

ranked = (
    tagged
    .select(
        "*",
        F.last("release_date_year").over(w_is_explicit_by_artist_debut).alias("last_release_date_year"),
        F.count(F.lit(1)).over(w_is_explicit_by_artist_debut_2).alias("running_count"),
        F.first("artist_debut_year").over(w_is_explicit_by_artist_debut).alias("first_artist_debut_year"),
    )
)

screened = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "is_explicit",
        "title_clean",
        "release_date_year",
        "artist_is_verified",
        "artist_debut_year",
        "title_clean_artist_country_label",
        "first_artist_debut_year",
    )
    .orderBy(F.col("title_clean_artist_country_label").desc())
)
