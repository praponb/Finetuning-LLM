from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_plus_7d_month = Window.partitionBy(F.month("release_date_plus_7d"))
w_artist_country_artist_id_by = Window.partitionBy("artist_country", "artist_id").orderBy("bpm", "id").rowsBetween(-2, -1)
w_artist_id_artist_genre_by_bp = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("bpm").desc(), "id")
w_artist_genre_by_artist_debut = Window.partitionBy("artist_genre").orderBy(F.col("artist_debut_year").asc_nulls_last(), "id")
w_release_date_plus_7d_month_a = Window.partitionBy(F.date_trunc("month", "release_date_plus_7d"), "artist_id").orderBy(F.col("artist_debut_year").desc_nulls_first(), "id")
w_artist_genre_by_artist_debut_2 = Window.partitionBy("artist_genre").orderBy(F.col("artist_debut_year").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_artist_id_artist_country = Window.partitionBy("artist_id", "artist_country")
w_artist_genre_by_artist_debut_3 = Window.partitionBy("artist_genre").orderBy(F.col("artist_debut_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_artist_genre_by_bp_2 = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_country_by_artist_deb = Window.partitionBy("artist_country").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_plus_7d_month_a_2 = Window.partitionBy(F.month("release_date_plus_7d"), "artist_genre").orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_plus_7d_month_a_3 = Window.partitionBy(F.month("release_date_plus_7d"), "artist_genre").orderBy(F.col("artist_debut_year").desc(), "id")
w_release_date_plus_7d_month_a_4 = Window.partitionBy(F.month("release_date_plus_7d"), "artist_genre").orderBy(F.col("artist_debut_year").desc())
w_distinct_artist_country_in_r = Window.partitionBy("distinct_artist_country_in_release_date_plus_7d_", "artist_country")
w_all_by_bpm_avg_2_prior_prior = Window.orderBy(F.col("bpm_avg_2_prior").desc(), "id").rowsBetween(-6, -1)

scoped = (
    df_tracks
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
    )
)

with_details = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.moods",
        "s.release_date_plus_7d",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.round(F.col("bpm") - F.avg("bpm").over(w_release_date_plus_7d_month), 2).alias("bpm_vs_release_date_plus_7d_month_avg"),
        F.round(F.avg("bpm").over(w_artist_country_artist_id_by), 2).alias("bpm_avg_2_prior"),
        F.ntile(10).over(w_artist_id_artist_genre_by_bp).alias("bpm_decile"),
        F.lag("artist_debut_year", 3).over(w_artist_genre_by_artist_debut).alias("prev3_artist_debut_year"),
        F.lag("bpm", 2).over(w_release_date_plus_7d_month_a).alias("prev2_bpm"),
        F.round(F.avg("bpm").over(w_artist_genre_by_artist_debut_2), 2).alias("bpm_avg_2"),
        F.size(F.collect_set("artist_country").over(w_release_date_plus_7d_month)).alias("distinct_artist_country_in_release_date_plus_7d_"),
        F.max("bpm").over(w_release_date_plus_7d_month).alias("release_date_plus_7d_month_max_bpm"),
        F.max("artist_debut_year").over(w_artist_id_artist_country).alias("artist_id_artist_country_max_artist_debut_year"),
        F.round(F.avg("bpm").over(w_artist_id_artist_country), 2).alias("artist_id_artist_country_avg_bpm"),
        F.max("bpm").over(w_artist_genre_by_artist_debut_3).alias("max_bpm_so_far"),
        F.last("bpm").over(w_artist_id_artist_genre_by_bp_2).alias("last_bpm"),
        F.last("artist_debut_year").over(w_artist_country_by_artist_deb).alias("last_artist_debut_year"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_id_artist_country), 2).alias("artist_debut_year_vs_artist_id_artist_country_av"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.first("last_bpm", ignorenulls=True).over(w_release_date_plus_7d_month_a_2).alias("first_known_last_bpm"),
        F.ntile(3).over(w_release_date_plus_7d_month_a_3).alias("artist_debut_year_tercile"),
        F.rank().over(w_release_date_plus_7d_month_a_4).alias("artist_debut_year_rank"),
        F.count(F.lit(1)).over(w_distinct_artist_country_in_r).alias("rows_in_distinct_artist_country_in_release_date_"),
        F.sum("last_bpm").over(w_all_by_bpm_avg_2_prior_prior).alias("last_bpm_sum_6_prior"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "bpm",
        "moods",
        "release_date_plus_7d",
        "artist_country",
        "artist_genre",
        "artist_debut_year",
        "bpm_avg_2_prior",
        "bpm_decile",
        "prev3_artist_debut_year",
        "prev2_bpm",
        "bpm_avg_2",
        "distinct_artist_country_in_release_date_plus_7d_",
        "max_bpm_so_far",
        "last_bpm",
        "last_artist_debut_year",
        "artist_debut_year_vs_artist_id_artist_country_av",
        "first_known_last_bpm",
        "artist_debut_year_rank",
        "rows_in_distinct_artist_country_in_release_date_",
        "last_bpm_sum_6_prior",
    )
    .orderBy(
        F.col("prev2_bpm").desc_nulls_first(),
        F.col("artist_debut_year_vs_artist_id_artist_country_av").desc(),
        "id",
    )
    .limit(5)
)
