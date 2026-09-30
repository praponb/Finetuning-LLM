from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("artist_debut_year").desc())
w_artist_id_by_release_date_pr = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(-6, -1)
w_artist_id_artist_is_verified_2 = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_artist_debut_year_ful = Window.orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy("release_date", "id")
w_all_by_duration_sec_moving_3 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_country_artist_is_ver = Window.partitionBy("artist_country", "artist_is_verified")
w_artist_country_is_explicit_b = Window.partitionBy("artist_country", "is_explicit").orderBy("release_date", "id")

filtered_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("duration_sec") <= 352))
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

with_parent_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_windows = (
    with_parent_artist
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "artist_country",
        "artist_is_verified",
        F.dense_rank().over(w_artist_id_artist_is_verified).alias("artist_debut_year_dense_rank"),
        F.round(F.avg("duration_sec").over(w_artist_id_by_release_date_pr), 2).alias("duration_sec_avg_6_prior"),
        F.nth_value("artist_debut_year", 2).over(w_artist_id_artist_is_verified_2).alias("second_artist_debut_year"),
        F.ntile(4).over(w_all_by_duration_sec).alias("duration_sec_quartile"),
        F.last("artist_debut_year").over(w_all_by_artist_debut_year_ful).alias("last_artist_debut_year"),
        F.ntile(5).over(w_artist_id_by_release_date).alias("release_date_quintile"),
        F.round(F.avg("artist_debut_year").over(w_all_by_duration_sec_moving_3), 2).alias("artist_debut_year_avg_4"),
        F.nth_value("duration_sec", 2).over(w_all_by_duration_sec_full).alias("second_duration_sec"),
        F.count(F.lit(1)).over(w_artist_country_artist_is_ver).alias("rows_in_artist_country_artist_is_verified"),
        F.rank().over(w_all_by_duration_sec).alias("duration_sec_rank"),
        F.ntile(10).over(w_artist_country_is_explicit_b).alias("release_date_decile"),
        F.lead("duration_sec").over(w_artist_country_is_explicit_b).alias("next_duration_sec"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("artist_debut_year_dense_rank") <= 3) & (F.col("release_date") < F.lit("2025-03-29").cast("date")))
    .drop("artist_debut_year_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "artist_id",
        "release_date",
        "artist_country",
        "artist_is_verified",
        "duration_sec_avg_6_prior",
        "second_artist_debut_year",
        "release_date_quintile",
        "second_duration_sec",
        "rows_in_artist_country_artist_is_verified",
        "duration_sec_rank",
        "release_date_decile",
    )
    .orderBy("second_duration_sec")
)
