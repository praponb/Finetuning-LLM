from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_is_explic = Window.partitionBy("artist_is_verified", "is_explicit").orderBy(F.col("artist_debut_year").desc(), "id")
w_artist_is_verified_is_explic_2 = Window.partitionBy("artist_is_verified", "is_explicit").orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tracks
    .filter((F.col("artist_id") <= 4) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

joined = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

without_matches = (
    joined.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.hour(F.col("p.played_at")).between(9, 15)) & (~F.col("p.is_skipped")), "left_anti")
)

sequenced = (
    without_matches
    .withColumn("next_artist_debut_year", F.lead("artist_debut_year", 1, 0).over(w_artist_is_verified_is_explic))
    .withColumn("min_artist_debut_year_so_far", F.min("artist_debut_year").over(w_artist_is_verified_is_explic_2))
)

result = (
    sequenced
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        "artist_debut_year",
        "min_artist_debut_year_so_far",
    )
    .orderBy(
        F.col("artist_debut_year").desc_nulls_first(),
        F.col("id").desc_nulls_first(),
    )
)
