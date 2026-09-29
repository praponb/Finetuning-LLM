from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id").orderBy("release_date", "id")
w_artist_is_verified_artist_id_2 = Window.partitionBy("artist_is_verified", "artist_id").orderBy("release_date")
w_artist_is_verified_artist_id_3 = Window.partitionBy("artist_is_verified", "artist_id").orderBy(F.col("release_date").desc())
w_artist_is_verified_artist_id_4 = Window.partitionBy("artist_is_verified", "artist_id").orderBy(F.col("release_date").desc(), "id")
w_artist_is_verified_artist_id_5 = Window.partitionBy("artist_is_verified", "artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_by_artist = Window.partitionBy(F.month("release_date")).orderBy(F.col("artist_debut_year").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_by_artist_2 = Window.partitionBy(F.month("release_date")).orderBy(F.col("artist_debut_year").desc_nulls_first())

base_tracks = (
    df_tracks
    .filter(F.col("bpm") < 155)
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
    )
)

with_details = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.moods",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.lead("artist_debut_year", 2).over(w_artist_is_verified_artist_id).alias("next2_artist_debut_year"),
        F.round(F.percent_rank().over(w_artist_is_verified_artist_id_2), 3).alias("release_date_percent_rank"),
        F.lead("release_date").over(w_artist_is_verified_artist_id).alias("next_release_date"),
        F.round(F.cume_dist().over(w_artist_is_verified_artist_id_3), 3).alias("release_date_cume_dist"),
        F.lag("artist_debut_year").over(w_artist_is_verified_artist_id_4).alias("prev_artist_debut_year"),
        F.nth_value("artist_debut_year", 2).over(w_artist_is_verified_artist_id_5).alias("second_artist_debut_year"),
        F.nth_value("release_date", 2).over(w_release_date_month_by_artist).alias("second_release_date"),
        F.round(F.cume_dist().over(w_artist_is_verified_artist_id_4), 3).alias("release_date_cume_dist_2"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_id), 2).alias("artist_debut_year_vs_artist_id_avg"),
        F.dense_rank().over(w_release_date_month_by_artist_2).alias("artist_debut_year_dense_rank"),
        F.dense_rank().over(w_artist_is_verified_artist_id_3).alias("release_date_dense_rank"),
        F.lead("artist_debut_year").over(w_artist_is_verified_artist_id).alias("next_artist_debut_year"),
    )
)

top_per_group = (
    ranked
    .filter(F.col("artist_debut_year_dense_rank") <= 5)
    .drop("artist_debut_year_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "artist_id",
        "release_date",
        "moods",
        "artist_is_verified",
        "artist_debut_year",
        "next2_artist_debut_year",
        "prev_artist_debut_year",
        "second_artist_debut_year",
        "second_release_date",
        "release_date_cume_dist_2",
        "artist_debut_year_vs_artist_id_avg",
        "release_date_dense_rank",
        "next_artist_debut_year",
    )
    .orderBy(F.col("next2_artist_debut_year").asc())
)
