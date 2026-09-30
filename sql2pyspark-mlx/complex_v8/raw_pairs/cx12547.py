from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm_ties = Window.orderBy("bpm")
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date"))
w_artist_id_artist_genre = Window.partitionBy("artist_id", "artist_genre")
w_artist_genre_artist_id_by_ar = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("artist_debut_year").desc_nulls_first(), "id")
w_artist_id_release_date_month_2 = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy("bpm", "id")
w_artist_id_release_date_month_3 = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy("bpm")
w_all_by_bpm = Window.orderBy("bpm", "id")

valid_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 344)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

combined_details = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.dense_rank().over(w_all_by_bpm_ties).alias("bpm_dense_rank"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_id_release_date_month), 2).alias("artist_debut_year_vs_artist_id_release_date_mont"),
        F.count(F.lit(1)).over(w_artist_id_artist_genre).alias("rows_in_artist_id_artist_genre"),
        F.size(F.collect_set("artist_genre").over(w_artist_id_release_date_month)).alias("distinct_artist_genre_in_artist_id_release_date_"),
        F.count(F.lit(1)).over(w_artist_id_release_date_month).alias("rows_in_artist_id_release_date_month"),
        F.round(F.avg("artist_debut_year").over(w_artist_id_artist_genre), 2).alias("artist_id_artist_genre_avg_artist_debut_year"),
        F.rank().over(w_artist_genre_artist_id_by_ar).alias("artist_debut_year_rank"),
        F.round(F.percent_rank().over(w_artist_id_release_date_month_2), 3).alias("bpm_percent_rank"),
        F.lag("bpm", 2).over(w_artist_id_release_date_month_2).alias("prev2_bpm"),
        F.rank().over(w_artist_id_release_date_month_3).alias("bpm_rank"),
        F.dense_rank().over(w_all_by_bpm).alias("bpm_dense_rank_2"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_artist_genre), F.lit(0)), 4).alias("bpm_share_of_artist_id_artist_genre"),
    )
)

grouped = (
    sequenced
    .groupBy(
        "bpm_dense_rank",
        "artist_id",
        F.date_trunc("month", "release_date").alias("release_date_month"),
    )
    .agg(
        F.round(F.avg("bpm_rank"), 2).alias("avg_bpm_rank"),
        F.sum("rows_in_artist_id_release_date_month").alias("total_rows_in_artist_id_release_date_month"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
        F.percentile_approx("rows_in_artist_id_release_date_month", 0.5).alias("median_rows_in_artist_id_release_date_month"),
        F.percentile_approx("bpm_rank", 0.5).alias("median_bpm_rank"),
        F.count("*").alias("cnt"),
    )
)

result = (
    grouped
    .select(
        "bpm_dense_rank",
        "artist_id",
        "release_date_span_days",
        "median_rows_in_artist_id_release_date_month",
        "cnt",
    )
    .orderBy(F.col("cnt").desc())
)
