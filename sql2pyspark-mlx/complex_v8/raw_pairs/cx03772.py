from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_release_date = Window.orderBy(F.col("release_date").asc_nulls_last(), "id")
w_artist_genre_by_release_date = Window.partitionBy("artist_genre").orderBy("release_date", "id")
w_artist_genre = Window.partitionBy("artist_genre")
w_all_by_release_date_full = Window.orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_artist_debut_ye_2 = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").desc_nulls_first())
w_artist_id_by_artist_debut_ye_3 = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_release_date_month_artist_ge = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_genre").orderBy(F.col("bpm").desc(), "id").rowsBetween(-2, Window.currentRow)
w_rn_by_artist_debut_year_cume = Window.partitionBy("rn").orderBy(F.col("artist_debut_year_cume_dist").desc(), "id").rowsBetween(-6, Window.currentRow)
w_rn_by_bpm_change = Window.partitionBy("rn").orderBy("bpm_change", "id")
w_distinct_artist_id_in_artist = Window.partitionBy("distinct_artist_id_in_artist_genre", "artist_id").orderBy("artist_debut_year_cume_dist", "id")

filtered = df_tracks.filter((F.col("is_explicit")) & (F.size(F.col("moods")) >= 1))

combined_details = (
    filtered.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.genre").isin("Pop", "HipHop")), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

windowed = (
    combined_details
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        "artist_name",
        "artist_debut_year",
        F.round(F.avg("bpm").over(w_artist_id_by_artist_debut_ye), 2).alias("bpm_avg_3"),
        F.row_number().over(w_all_by_release_date).alias("rn"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_genre_by_release_date)).alias("bpm_change"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_genre), 2).alias("artist_debut_year_vs_artist_genre_avg"),
        F.nth_value("bpm", 2).over(w_all_by_release_date_full).alias("second_bpm"),
        F.round(F.cume_dist().over(w_artist_id_by_artist_debut_ye_2), 3).alias("artist_debut_year_cume_dist"),
        F.round(F.avg("artist_debut_year").over(w_artist_id_by_artist_debut_ye_3), 2).alias("artist_debut_year_avg_2_prior"),
        F.sum("bpm").over(w_artist_genre).alias("artist_genre_sum_bpm"),
        F.min("artist_debut_year").over(w_artist_genre).alias("artist_genre_min_artist_debut_year"),
        F.round(F.avg("bpm").over(w_release_date_month_artist_ge), 2).alias("bpm_avg_3_2"),
        F.size(F.collect_set("artist_id").over(w_artist_genre)).alias("distinct_artist_id_in_artist_genre"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        "bpm_change",
        "artist_debut_year_vs_artist_genre_avg",
        "artist_debut_year_cume_dist",
        "artist_debut_year_avg_2_prior",
        "artist_genre_sum_bpm",
        "bpm_avg_3_2",
        F.round(F.avg("second_bpm").over(w_rn_by_artist_debut_year_cume), 2).alias("second_bpm_avg_7"),
        F.round(F.percent_rank().over(w_rn_by_bpm_change), 3).alias("bpm_change_percent_rank"),
        F.ntile(10).over(w_distinct_artist_id_in_artist).alias("artist_debut_year_cume_dist_decile"),
        (F.col("bpm_change") - F.lag("bpm_change").over(w_rn_by_bpm_change)).alias("bpm_change_change"),
    )
)

result = (
    with_windows
    .filter(F.col("release_date") >= F.lit("2024-06-03").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        "bpm_change",
        "artist_debut_year_vs_artist_genre_avg",
        "artist_debut_year_cume_dist",
        "artist_debut_year_avg_2_prior",
        "artist_genre_sum_bpm",
        "second_bpm_avg_7",
        "artist_debut_year_cume_dist_decile",
        "bpm_change_change",
    )
    .orderBy("second_bpm_avg_7", "title")
)
