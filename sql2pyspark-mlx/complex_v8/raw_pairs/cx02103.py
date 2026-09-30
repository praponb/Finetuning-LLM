from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy("artist_debut_year")
w_all_by_artist_debut_year = Window.orderBy(F.col("artist_debut_year").asc_nulls_last(), "id")
w_release_date_year_by_release = Window.partitionBy(F.year("release_date")).orderBy("release_date", "id")
w_artist_id_by_artist_debut_ye_2 = Window.partitionBy("artist_id").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_rows_ties = Window.orderBy("n_rows")
w_all_by_n_rows_moving_3 = Window.orderBy("n_rows", "release_date_month", "next2_release_date_year", "prev3_release_date_year").rowsBetween(-3, Window.currentRow)

base = (
    df_tracks
    .filter(F.col("artist_id") >= 9)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
    )
)

with_details = (
    base.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year") >= 1996)
    .select("s.*", F.col("a.debut_year").alias("artist_debut_year"))
)

with_trends = (
    with_details
    .select(
        "*",
        F.round(F.avg("artist_debut_year").over(w_artist_id), 2).alias("artist_id_avg_artist_debut_year"),
        F.min("artist_debut_year").over(w_artist_id).alias("artist_id_min_artist_debut_year"),
        F.round(F.cume_dist().over(w_artist_id_by_artist_debut_ye), 3).alias("artist_debut_year_cume_dist"),
        F.lead("release_date", 2).over(w_all_by_artist_debut_year).alias("next2_release_date"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_artist_id), F.lit(0)), 4).alias("artist_debut_year_share_of_artist_id"),
        F.lag("release_date", 3).over(w_release_date_year_by_release).alias("prev3_release_date"),
        F.min("artist_debut_year").over(w_artist_id_by_artist_debut_ye_2).alias("min_artist_debut_year_so_far"),
        F.sum("artist_debut_year").over(w_artist_id_by_artist_debut_ye_2).alias("running_artist_debut_year"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_id), 2).alias("artist_debut_year_vs_artist_id_avg"),
        F.round(F.percent_rank().over(w_release_date_year_by_release), 3).alias("release_date_percent_rank"),
    )
)

summary = (
    with_trends
    .groupBy(
        F.date_trunc("month", "release_date").alias("release_date_month"),
        F.year("next2_release_date").alias("next2_release_date_year"),
        F.year("prev3_release_date").alias("prev3_release_date_year"),
    )
    .agg(
        F.percentile_approx("running_artist_debut_year", 0.5).alias("median_running_artist_debut_year"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.avg("release_date_percent_rank").alias("avg_release_date_percent_rank_check"),
    )
    .filter(F.col("avg_release_date_percent_rank_check") > 0.4)
    .drop("avg_release_date_percent_rank_check")
)

with_trends_2 = (
    summary
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_n_rows_ties), 3).alias("n_rows_cume_dist"),
        F.sum("prev3_release_date_year").over(w_all_by_n_rows_moving_3).alias("prev3_release_date_year_sum_4"),
    )
)

result = (
    with_trends_2
    .select(
        "release_date_month",
        "prev3_release_date_year",
        "median_running_artist_debut_year",
        "n_rows",
        "title_list",
        "n_rows_cume_dist",
        "prev3_release_date_year_sum_4",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date_month")).alias("days_since_release_date_month"),
        F.to_date(F.col("release_date_month")).alias("release_date_month_date"),
    )
    .orderBy(F.col("median_running_artist_debut_year").asc_nulls_last())
)
