from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre_country_by_debut_year = Window.partitionBy("genre", "country").orderBy(F.col("debut_year").desc(), "id")
w_country_genre = Window.partitionBy("country", "genre")
w_genre_country_by_debut_year_2 = Window.partitionBy("genre", "country").orderBy(F.col("debut_year").desc())
w_genre_debut_year_rank_by_deb = Window.partitionBy("genre", "debut_year_rank").orderBy(F.col("debut_year").desc(), "id").rowsBetween(-6, -1)
w_debut_year_rank_2 = Window.partitionBy("debut_year_rank_2")
w_genre_debut_year_rank_by_deb_2 = Window.partitionBy("genre", "debut_year_rank").orderBy(F.col("debut_year").desc())
w_distinct_country_in_debut_ye = Window.partitionBy("distinct_country_in_debut_year_rank_2").orderBy(F.col("debut_year_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_debut_year_dense_rank = Window.partitionBy("debut_year_dense_rank")
w_distinct_country_in_debut_ye_2 = Window.partitionBy("distinct_country_in_debut_year_rank_2").orderBy(F.col("debut_year_rank").desc(), "id")
w_distinct_country_in_debut_ye_3 = Window.partitionBy("distinct_country_in_debut_year_rank_2").orderBy(F.col("debut_year_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_median_debut_year_rank_disti = Window.partitionBy("median_debut_year_rank", "distinct_genres")
w_max_debut_year_rank_2_by_med = Window.partitionBy("max_debut_year_rank_2").orderBy("median_debut_year_rank", "debut_year_rank", "debut_year_dense_rank")
w_max_debut_year_rank_2_by_med_2 = Window.partitionBy("max_debut_year_rank_2").orderBy("median_debut_year_rank")

filtered = (
    df_artists
    .filter(F.col("country") != "BR")
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        F.concat_ws("|", F.col("artist_name"), F.col("genre")).alias("artist_name_genre_label"),
        F.concat(F.col("genre"), F.lit("#"), F.col("debut_year").cast("string")).alias("genre_ref"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.rank().over(w_genre_country_by_debut_year).alias("debut_year_rank"),
        F.sum("debut_year").over(w_country_genre).alias("country_genre_sum_debut_year"),
        F.rank().over(w_genre_country_by_debut_year_2).alias("debut_year_rank_2"),
        F.lag("debut_year").over(w_genre_country_by_debut_year).alias("prev_debut_year"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "genre",
        "country",
        "artist_name_genre_label",
        "debut_year_rank",
        "country_genre_sum_debut_year",
        "debut_year_rank_2",
        F.round(F.avg("debut_year").over(w_genre_debut_year_rank_by_deb), 2).alias("debut_year_avg_6_prior"),
        F.size(F.collect_set("country").over(w_debut_year_rank_2)).alias("distinct_country_in_debut_year_rank_2"),
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_debut_year_rank_2), 2).alias("debut_year_vs_debut_year_rank_2_avg"),
        F.dense_rank().over(w_genre_debut_year_rank_by_deb_2).alias("debut_year_dense_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.last("debut_year_rank").over(w_distinct_country_in_debut_ye).alias("last_debut_year_rank"),
        F.round(F.avg("country_genre_sum_debut_year").over(w_debut_year_dense_rank), 2).alias("debut_year_dense_rank_avg_country_genre_sum_debu"),
        F.lead("debut_year_dense_rank", 2).over(w_distinct_country_in_debut_ye_2).alias("next2_debut_year_dense_rank"),
        F.last("debut_year_rank_2", ignorenulls=True).over(w_distinct_country_in_debut_ye_3).alias("last_known_debut_year_rank_2"),
        F.count(F.lit(1)).over(w_debut_year_dense_rank).alias("rows_in_debut_year_dense_rank"),
        (F.col("debut_year_rank_2") - F.lag("debut_year_rank_2").over(w_distinct_country_in_debut_ye_2)).alias("debut_year_rank_2_change"),
    )
)

grouped = (
    metrics
    .groupBy("debut_year_rank", "debut_year_dense_rank")
    .agg(
        F.percentile_approx("debut_year_rank", 0.5).alias("median_debut_year_rank"),
        F.max("debut_year_rank_2").alias("max_debut_year_rank_2"),
        F.countDistinct("genre").alias("distinct_genres"),
        F.sum(F.when(F.col("country") == "BR", F.col("debut_year_avg_6_prior")).otherwise(0)).alias("debut_year_avg_6_prior_country_br"),
        F.avg("debut_year_rank").alias("avg_debut_year_rank_check"),
    )
    .filter(F.col("avg_debut_year_rank_check") > 2)
    .drop("avg_debut_year_rank_check")
)

sequenced = (
    grouped
    .select(
        "debut_year_rank",
        "debut_year_dense_rank",
        "median_debut_year_rank",
        "max_debut_year_rank_2",
        "distinct_genres",
        "debut_year_avg_6_prior_country_br",
        F.round(F.col("distinct_genres") / F.nullif(F.sum("distinct_genres").over(w_median_debut_year_rank_disti), F.lit(0)), 4).alias("distinct_genres_share_of_median_debut_year_rank_"),
        F.dense_rank().over(w_max_debut_year_rank_2_by_med).alias("median_debut_year_rank_dense_rank"),
        F.dense_rank().over(w_max_debut_year_rank_2_by_med_2).alias("median_debut_year_rank_dense_rank_2"),
    )
)

without_matches = (
    sequenced
    .crossJoin(sequenced.agg(F.min("debut_year_dense_rank").alias("overall_min_debut_year_dense_rank")))
    .filter(F.col("debut_year_dense_rank") > F.col("overall_min_debut_year_dense_rank"))
    .drop("overall_min_debut_year_dense_rank")
)

result = (
    without_matches
    .filter(F.col("max_debut_year_rank_2") <= 9)
    .select(
        "debut_year_rank",
        "median_debut_year_rank",
        "max_debut_year_rank_2",
        "distinct_genres",
        "median_debut_year_rank_dense_rank",
        "median_debut_year_rank_dense_rank_2",
        F.abs(F.col("median_debut_year_rank") - 6).alias("median_debut_year_rank_distance"),
    )
    .orderBy(
        F.col("max_debut_year_rank_2").desc(),
        F.col("median_debut_year_rank_distance").asc_nulls_last(),
    )
)
