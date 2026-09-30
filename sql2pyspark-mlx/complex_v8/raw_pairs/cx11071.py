from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_is_verified_by_debut = Window.partitionBy("country", "is_verified").orderBy(F.col("debut_year_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_country_is_verified_by_debut_2 = Window.partitionBy("country", "is_verified").orderBy(F.col("debut_year_filled").desc())
w_is_verified_by_debut_year_fu = Window.partitionBy("is_verified").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_genre_by_debut_year_full = Window.partitionBy("genre").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_verified_by_debut_year_ru = Window.partitionBy("is_verified").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_genre_is_verified_by_debut_y = Window.partitionBy("genre", "is_verified").orderBy(F.col("debut_year").desc(), "id")
w_is_verified = Window.partitionBy("is_verified")

prepared = (
    df_artists
    .filter(F.col("country") == "BR")
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        F.nth_value("debut_year", 2).over(w_country_is_verified_by_debut).alias("second_debut_year"),
        F.dense_rank().over(w_country_is_verified_by_debut_2).alias("debut_year_filled_dense_rank"),
        F.last("debut_year_filled").over(w_is_verified_by_debut_year_fu).alias("last_debut_year_filled"),
        F.last("debut_year_filled").over(w_genre_by_debut_year_full).alias("last_debut_year_filled_2"),
        F.first("debut_year").over(w_genre_by_debut_year_full).alias("first_debut_year"),
        F.min("debut_year").over(w_is_verified_by_debut_year_ru).alias("min_debut_year_so_far"),
        F.lag("debut_year").over(w_genre_is_verified_by_debut_y).alias("prev_debut_year"),
        F.size(F.collect_set("genre").over(w_is_verified)).alias("distinct_genre_in_is_verified"),
    )
)

summary = (
    metrics
    .groupBy("distinct_genre_in_is_verified", "debut_year_filled_dense_rank")
    .agg(
        F.percentile_approx("prev_debut_year", 0.5).alias("median_prev_debut_year"),
        F.sum(F.when(F.col("country") == "US", F.col("debut_year_filled_dense_rank"))).alias("debut_year_filled_dense_rank_country_us"),
        F.sum(F.when(F.col("country") == "KR", 1).otherwise(0)).alias("n_country_kr"),
        F.avg("last_debut_year_filled").alias("avg_last_debut_year_filled_check"),
    )
    .filter(F.col("avg_last_debut_year_filled_check") > 2003)
    .drop("avg_last_debut_year_filled_check")
)

result = (
    summary
    .select(
        "distinct_genre_in_is_verified",
        "debut_year_filled_dense_rank",
        "debut_year_filled_dense_rank_country_us",
        F.least(F.col("n_country_kr"), F.col("median_prev_debut_year")).alias("least_n_country_kr_median_prev_debut_year"),
    )
    .orderBy(
        F.col("distinct_genre_in_is_verified").desc_nulls_first(),
        F.col("debut_year_filled_dense_rank").desc(),
    )
)
