from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year_artist_is = Window.partitionBy("release_date_year", "artist_is_verified")
w_all_by_artist_debut_year = Window.orderBy(F.col("artist_debut_year").asc_nulls_last(), "id")
w_artist_genre_by_release_date = Window.partitionBy("artist_genre").orderBy("release_date_year", "id").rowsBetween(-3, -1)
w_all_by_release_date_year = Window.orderBy("release_date_year", "id")
w_artist_genre_by_release_date_2 = Window.partitionBy("artist_genre").orderBy("release_date_year", "id")
w_all_by_artist_debut_year_tie = Window.orderBy(F.col("artist_debut_year").asc_nulls_last())
w_artist_genre_by_release_date_3 = Window.partitionBy("artist_genre").orderBy("release_date_year", "id").rowsBetween(-3, Window.currentRow)
w_all_by_release_date_year_run = Window.orderBy("release_date_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_artist_debut_year_2 = Window.orderBy(F.col("artist_debut_year").desc(), "id")
w_release_date_year_artist_id = Window.partitionBy("release_date_year", "artist_id").orderBy("artist_debut_year", "id")
w_artist_is_verified_release_d = Window.partitionBy("artist_is_verified", "release_date_year")
w_artist_genre_by_release_date_4 = Window.partitionBy("artist_genre").orderBy("release_date_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_year_artist_id_2 = Window.partitionBy("release_date_year", "artist_id").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "focus")) | (F.col("duration_sec") >= 239))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.year(F.col("release_date")).alias("release_date_year"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

combined_details = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.release_date_year",
        "s.bpm_filled",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_release_date_year_artist_is)).alias("distinct_artist_id_in_release_date_year_artist_i"),
        F.lag("release_date_year").over(w_all_by_artist_debut_year).alias("prev_release_date_year"),
        F.max("release_date_year").over(w_artist_genre_by_release_date).alias("release_date_year_max_3_prior"),
        F.ntile(4).over(w_all_by_release_date_year).alias("release_date_year_quartile"),
        F.lag("artist_debut_year", 2).over(w_artist_genre_by_release_date_2).alias("prev2_artist_debut_year"),
        F.dense_rank().over(w_all_by_artist_debut_year_tie).alias("artist_debut_year_dense_rank"),
        F.round(F.avg("duration_sec").over(w_artist_genre_by_release_date_3), 2).alias("duration_sec_avg_4"),
        F.sum("release_date_year").over(w_all_by_release_date_year_run).alias("running_release_date_year"),
        F.lead("artist_debut_year", 3, 0).over(w_all_by_artist_debut_year_2).alias("next3_artist_debut_year"),
        F.lag("release_date").over(w_release_date_year_artist_id).alias("prev_release_date"),
        F.round(F.col("bpm_filled") - F.avg("bpm_filled").over(w_artist_is_verified_release_d), 2).alias("bpm_filled_vs_artist_is_verified_release_date_ye"),
        F.nth_value("release_date", 2).over(w_artist_genre_by_release_date_4).alias("second_release_date"),
        F.round(F.percent_rank().over(w_artist_genre_by_release_date_2), 3).alias("release_date_year_percent_rank"),
        F.lead("release_date_year", 1, 0).over(w_all_by_release_date_year).alias("next_release_date_year"),
        F.min("release_date_year").over(w_release_date_year_artist_id_2).alias("min_release_date_year_so_far"),
    )
)

leaders = windowed.filter(F.col("artist_debut_year_dense_rank") <= 3)

result = (
    leaders
    .filter(F.col("duration_sec_avg_4") >= 172.5)
    .select(
        "id",
        "artist_id",
        "release_date",
        "release_date_year",
        "bpm_filled",
        "artist_debut_year",
        "artist_is_verified",
        "distinct_artist_id_in_release_date_year_artist_i",
        "release_date_year_quartile",
        "artist_debut_year_dense_rank",
        "duration_sec_avg_4",
        "next3_artist_debut_year",
        "prev_release_date",
        "bpm_filled_vs_artist_is_verified_release_date_ye",
        "second_release_date",
        "release_date_year_percent_rank",
        "next_release_date_year",
        "min_release_date_year_so_far",
        F.round(F.col("release_date_year") / F.nullif(F.col("artist_debut_year"), F.lit(0)), 3).alias("release_date_year_per_artist_debut_year"),
    )
    .orderBy(
        F.col("release_date_year_quartile").desc(),
        F.col("next3_artist_debut_year").desc(),
        "id",
    )
    .limit(5)
)
