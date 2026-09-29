from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date = Window.orderBy("release_date", "id")
w_all_by_bpm_distance = Window.orderBy("bpm_distance", "id")
w_artist_country_is_explicit = Window.partitionBy("artist_country", "is_explicit")
w_all_by_bpm_distance_prior_2 = Window.orderBy("bpm_distance", "id").rowsBetween(-2, -1)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_artist_country_b = Window.partitionBy("is_explicit", "artist_country").orderBy("release_date")
w_all_by_bpm_distance_running = Window.orderBy("bpm_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("duration_sec").between(165, 352)))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("bpm") - 118).alias("bpm_distance"),
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.bpm_distance",
        "s.title_ref",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
    )
)

with_trends = (
    with_details
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        "bpm_distance",
        "title_ref",
        "artist_is_verified",
        F.nth_value("artist_debut_year", 2).over(w_is_explicit_by_release_date).alias("second_artist_debut_year"),
        F.rank().over(w_all_by_release_date).alias("release_date_rank"),
        F.lead("artist_debut_year", 2).over(w_all_by_bpm_distance).alias("next2_artist_debut_year"),
        F.round(F.col("bpm_distance") / F.nullif(F.sum("bpm_distance").over(w_artist_country_is_explicit), F.lit(0)), 4).alias("bpm_distance_share_of_artist_country_is_explicit"),
        F.sum("bpm_distance").over(w_all_by_bpm_distance_prior_2).alias("bpm_distance_sum_2_prior"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.dense_rank().over(w_is_explicit_artist_country_b).alias("release_date_dense_rank"),
        F.max("bpm_distance").over(w_all_by_bpm_distance_running).alias("max_bpm_distance_so_far"),
        F.last("bpm_distance", ignorenulls=True).over(w_is_explicit_by_release_date_2).alias("last_known_bpm_distance"),
    )
)

labeled = (
    with_trends
    .filter(F.col("second_artist_debut_year").between(1996, 2020))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        "bpm_distance",
        "title_ref",
        "second_artist_debut_year",
        "release_date_rank",
        "bpm_distance_share_of_artist_country_is_explicit",
        "bpm_distance_sum_2_prior",
        "rows_in_artist_id_is_explicit",
        "release_date_dense_rank",
        "max_bpm_distance_so_far",
        "last_known_bpm_distance",
        F.concat(F.col("title_ref"), F.lit("#"), F.col("next2_artist_debut_year").cast("string")).alias("title_ref_ref"),
        F.substring(F.trim(F.col("title_ref")), 1, 3).alias("title_ref_prefix"),
    )
)

result = (
    labeled
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "moods",
        "bpm_distance",
        "second_artist_debut_year",
        "release_date_rank",
        "bpm_distance_share_of_artist_country_is_explicit",
        "bpm_distance_sum_2_prior",
        "rows_in_artist_id_is_explicit",
        "release_date_dense_rank",
        "last_known_bpm_distance",
        "title_ref_ref",
        "title_ref_prefix",
        (F.floor(F.col("second_artist_debut_year") / 5) * 5).alias("second_artist_debut_year_bucket"),
    )
    .orderBy("rows_in_artist_id_is_explicit", F.col("release_date_rank").asc())
)
