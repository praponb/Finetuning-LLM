from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_debut_year_running = Window.orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_debut_year_full = Window.orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_verified_genre_by_debut_y = Window.partitionBy("is_verified", "genre").orderBy("debut_year")
w_genre_or_unknown_country_by = Window.partitionBy("genre_or_unknown", "country").orderBy("artist_name_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_genre_or_unknown_country_by_2 = Window.partitionBy("genre_or_unknown", "country").orderBy("artist_name_len")
w_genre_or_unknown = Window.partitionBy("genre_or_unknown")
w_genre_genre_or_unknown = Window.partitionBy("genre", "genre_or_unknown")
w_all_by_debut_year_ties = Window.orderBy("debut_year")
w_genre_or_unknown_country_by_3 = Window.partitionBy("genre_or_unknown", "country").orderBy("artist_name_len", "id").rowsBetween(-1, Window.currentRow)
w_artist_name_len_by_artist_na = Window.partitionBy("artist_name_len").orderBy(F.col("artist_name_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_debut_year = Window.orderBy("debut_year", "id")
w_country = Window.partitionBy("country")

recent = (
    df_artists
    .filter(F.col("genre").isNotNull())
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.length(F.trim(F.col("artist_name"))).alias("artist_name_len"),
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.first("artist_name_len", ignorenulls=True).over(w_all_by_debut_year_running).alias("first_known_artist_name_len"),
        F.last("artist_name_len").over(w_all_by_debut_year_full).alias("last_artist_name_len"),
        F.round(F.cume_dist().over(w_is_verified_genre_by_debut_y), 3).alias("debut_year_cume_dist"),
        F.first("artist_name_len").over(w_genre_or_unknown_country_by).alias("first_artist_name_len"),
        F.round(F.percent_rank().over(w_genre_or_unknown_country_by_2), 3).alias("artist_name_len_percent_rank"),
        F.sum("artist_name_len").over(w_genre_or_unknown).alias("genre_or_unknown_sum_artist_name_len"),
        F.count(F.lit(1)).over(w_genre_genre_or_unknown).alias("rows_in_genre_genre_or_unknown"),
        F.dense_rank().over(w_all_by_debut_year_ties).alias("debut_year_dense_rank"),
        F.round(F.avg("debut_year").over(w_genre_or_unknown_country_by_3), 2).alias("debut_year_avg_2"),
        F.last("debut_year").over(w_artist_name_len_by_artist_na).alias("last_debut_year"),
        F.nth_value("artist_name_len", 2).over(w_all_by_debut_year_full).alias("second_artist_name_len"),
        F.row_number().over(w_all_by_debut_year).alias("debut_year_seq"),
        F.count(F.lit(1)).over(w_country).alias("rows_in_country"),
    )
)

result = (
    metrics
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        "artist_name_len",
        "genre_or_unknown",
        "first_known_artist_name_len",
        "last_artist_name_len",
        "debut_year_cume_dist",
        "first_artist_name_len",
        "artist_name_len_percent_rank",
        "genre_or_unknown_sum_artist_name_len",
        "rows_in_genre_genre_or_unknown",
        "debut_year_dense_rank",
        "last_debut_year",
        "second_artist_name_len",
        "debut_year_seq",
        "rows_in_country",
        F.least(F.col("debut_year_avg_2"), F.col("first_artist_name_len")).alias("least_debut_year_avg_2_first_artist_name_len"),
    )
    .orderBy(
        F.col("artist_name_len_percent_rank").asc(),
        F.col("first_known_artist_name_len").desc(),
    )
)
