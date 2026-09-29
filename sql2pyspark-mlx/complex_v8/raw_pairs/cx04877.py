from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre = Window.partitionBy("artist_genre")
w_artist_id_artist_genre_by_du = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_artist_genre_by_du_2 = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-6, Window.currentRow)
w_artist_id_by_duration_sec_pr = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_artist_genre_artist_id_by_du = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_duration_sec_rank_artist_id = Window.partitionBy("duration_sec_rank", "artist_id")
w_all_by_duration_sec_max_7_mo = Window.orderBy(F.col("duration_sec_max_7").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_duration_sec_max_7 = Window.orderBy(F.col("duration_sec_max_7").desc(), "id")
w_duration_sec_dense_rank_arti = Window.partitionBy("duration_sec_dense_rank", "artist_genre").orderBy(F.col("duration_sec_max_7").asc_nulls_last())
w_duration_sec_dense_rank_arti_2 = Window.partitionBy("duration_sec_dense_rank", "artist_genre").orderBy(F.col("duration_sec_max_7").asc_nulls_last(), "id").rowsBetween(-3, -1)

valid = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-03-29").cast("date")) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

combined_details_artist = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.title_slug",
        F.col("a.genre").alias("artist_genre"),
    )
)

with_windows = (
    combined_details_artist
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_artist_genre)).alias("distinct_artist_id_in_artist_genre"),
        F.ntile(4).over(w_artist_id_artist_genre_by_du).alias("duration_sec_quartile"),
        F.min("duration_sec").over(w_artist_genre).alias("artist_genre_min_duration_sec"),
        F.max("duration_sec").over(w_artist_id_artist_genre_by_du_2).alias("duration_sec_max_7"),
        F.count(F.lit(1)).over(w_artist_genre).alias("rows_in_artist_genre"),
        F.rank().over(w_artist_id_artist_genre_by_du).alias("duration_sec_rank"),
        F.sum("duration_sec").over(w_artist_id_by_duration_sec_pr).alias("duration_sec_sum_3_prior"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_genre), 2).alias("duration_sec_vs_artist_genre_avg"),
        F.dense_rank().over(w_artist_id_artist_genre_by_du).alias("duration_sec_dense_rank"),
        F.nth_value("duration_sec", 2).over(w_artist_genre_artist_id_by_du).alias("second_duration_sec"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "title_slug",
        "artist_genre",
        "distinct_artist_id_in_artist_genre",
        "duration_sec_quartile",
        "artist_genre_min_duration_sec",
        "rows_in_artist_genre",
        "duration_sec_sum_3_prior",
        "duration_sec_vs_artist_genre_avg",
        F.round(F.avg("duration_sec_dense_rank").over(w_duration_sec_rank_artist_id), 2).alias("duration_sec_rank_artist_id_avg_duration_sec_den"),
        F.sum("duration_sec_dense_rank").over(w_all_by_duration_sec_max_7_mo).alias("duration_sec_dense_rank_sum_4"),
        F.max("duration_sec_sum_3_prior").over(w_duration_sec_rank_artist_id).alias("duration_sec_rank_artist_id_max_duration_sec_sum"),
        (F.col("duration_sec_dense_rank") - F.lag("duration_sec_dense_rank").over(w_all_by_duration_sec_max_7)).alias("duration_sec_dense_rank_change"),
        F.ntile(4).over(w_all_by_duration_sec_max_7).alias("duration_sec_max_7_quartile"),
        F.rank().over(w_duration_sec_dense_rank_arti).alias("duration_sec_max_7_rank"),
        F.sum("duration_sec_max_7").over(w_duration_sec_dense_rank_arti_2).alias("duration_sec_max_7_sum_3_prior"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "title_slug",
        "artist_genre",
        "distinct_artist_id_in_artist_genre",
        "duration_sec_quartile",
        "artist_genre_min_duration_sec",
        "rows_in_artist_genre",
        "duration_sec_sum_3_prior",
        "duration_sec_rank_artist_id_avg_duration_sec_den",
        "duration_sec_rank_artist_id_max_duration_sec_sum",
        "duration_sec_dense_rank_change",
        "duration_sec_max_7_quartile",
        "duration_sec_max_7_rank",
        "duration_sec_max_7_sum_3_prior",
        (F.col("duration_sec_max_7_rank") + F.col("rows_in_artist_genre")).alias("duration_sec_max_7_rank_plus_rows_in_artist_genr"),
        F.concat_ws(": ", F.col("artist_genre"), F.col("title_slug")).alias("artist_genre_title_slug_label"),
    )
    .orderBy(F.col("duration_sec_max_7_sum_3_prior").asc_nulls_last())
)
