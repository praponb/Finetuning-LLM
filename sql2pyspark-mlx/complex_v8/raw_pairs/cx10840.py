from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre_len_is_verified_by_deb = Window.partitionBy("genre_len", "is_verified").orderBy(F.col("debut_year").desc(), "id")
w_all_by_genre_len = Window.orderBy(F.col("genre_len").desc(), "id")
w_genre_genre_len = Window.partitionBy("genre", "genre_len")
w_genre_len_is_verified_by_deb_2 = Window.partitionBy("genre_len", "is_verified").orderBy(F.col("debut_year").desc())
w_is_verified_genre_len_by_gen = Window.partitionBy("is_verified", "genre_len").orderBy(F.col("genre_len").desc(), "id").rowsBetween(-1, -1)
w_is_verified_genre_len_by_gen_2 = Window.partitionBy("is_verified", "genre_len").orderBy(F.col("genre_len").desc())
w_all_by_debut_year_ties = Window.orderBy("debut_year")
w_is_verified = Window.partitionBy("is_verified")

valid_artists = (
    df_artists
    .filter((~F.col("is_verified")) & (F.length(F.trim(F.col("artist_name"))) >= 6))
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "is_verified",
        F.length(F.trim(F.col("genre"))).alias("genre_len"),
    )
)

sequenced = (
    valid_artists
    .select(
        "id",
        "artist_name",
        "debut_year",
        "genre_len",
        (F.col("debut_year") - F.lag("debut_year").over(w_genre_len_is_verified_by_deb)).alias("debut_year_change"),
        (F.col("genre_len") - F.lag("genre_len").over(w_all_by_genre_len)).alias("genre_len_change"),
        F.count(F.lit(1)).over(w_genre_genre_len).alias("rows_in_genre_genre_len"),
        F.rank().over(w_genre_len_is_verified_by_deb_2).alias("debut_year_rank"),
        F.round(F.avg("debut_year").over(w_is_verified_genre_len_by_gen), 2).alias("debut_year_avg_1_prior"),
        F.round(F.percent_rank().over(w_is_verified_genre_len_by_gen_2), 3).alias("genre_len_percent_rank"),
        F.dense_rank().over(w_is_verified_genre_len_by_gen_2).alias("genre_len_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_debut_year_ties), 3).alias("debut_year_percent_rank"),
        F.min("genre_len").over(w_is_verified).alias("is_verified_min_genre_len"),
    )
)

rollup_base = (
    sequenced
    .filter(F.col("debut_year_rank") < 9)
    .groupBy("genre_len_dense_rank", "genre_len")
    .agg(
        F.max("debut_year").alias("max_debut_year"),
        F.count(F.when(F.upper(F.trim(F.col("artist_name"))).like("K%"), 1)).alias("n_upper_trim_artist_name_like"),
        F.sum("genre_len_dense_rank").alias("total_genre_len_dense_rank"),
        F.max("rows_in_genre_genre_len").alias("max_rows_in_genre_genre_len_check"),
    )
    .filter(F.col("max_rows_in_genre_genre_len_check") > 5)
    .drop("max_rows_in_genre_genre_len_check")
)

result = (
    rollup_base
    .select(
        "genre_len_dense_rank",
        "genre_len",
        "max_debut_year",
        "n_upper_trim_artist_name_like",
        F.coalesce(F.col("genre_len_dense_rank"), F.lit(0)).alias("genre_len_dense_rank_filled"),
        F.coalesce(F.col("genre_len"), F.lit(0)).alias("genre_len_filled"),
    )
    .orderBy(F.col("genre_len_dense_rank_filled").desc())
)
