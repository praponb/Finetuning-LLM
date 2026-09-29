from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified = Window.partitionBy("is_verified")
w_is_verified_genre_by_n_count = Window.partitionBy("is_verified", "genre").orderBy("n_country_not_in_jp_kr", "genre", "is_verified", "country").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_country_by_stddev_deb = Window.partitionBy("n_rows", "country").orderBy("stddev_debut_year", "genre", "is_verified", "country")
w_n_rows_country_by_stddev_deb_2 = Window.partitionBy("n_rows", "country").orderBy("stddev_debut_year")

valid = df_artists.filter(F.col("debut_year") <= 2016)

stats = (
    valid
    .groupBy("genre", "is_verified", "country")
    .agg(
        F.round(F.stddev("debut_year"), 2).alias("stddev_debut_year"),
        F.count(F.when(~F.col("country").isin("JP", "KR"), 1)).alias("n_country_not_in_jp_kr"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("country")).alias("country_list"),
        F.percentile_approx("debut_year", 0.5).alias("median_debut_year"),
    )
)

screened = (
    stats
    .crossJoin(stats.agg(F.min("median_debut_year").alias("overall_min_median_debut_year")))
    .filter(F.col("median_debut_year") > F.col("overall_min_median_debut_year"))
    .drop("overall_min_median_debut_year")
)

qualified = (
    screened
    .crossJoin(screened.agg(F.max("stddev_debut_year").alias("overall_max_stddev_debut_year")))
    .filter(F.col("stddev_debut_year") < F.col("overall_max_stddev_debut_year"))
    .drop("overall_max_stddev_debut_year")
)

screened_2 = (
    qualified
    .crossJoin(qualified.agg(F.avg("n_rows").alias("overall_avg_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_avg_n_rows"))
    .drop("overall_avg_n_rows")
)

windowed = (
    screened_2
    .select(
        "genre",
        "is_verified",
        "country",
        "stddev_debut_year",
        "n_rows",
        "country_list",
        "median_debut_year",
        F.round(F.col("stddev_debut_year") - F.avg("stddev_debut_year").over(w_is_verified), 2).alias("stddev_debut_year_vs_is_verified_avg"),
        F.first("stddev_debut_year", ignorenulls=True).over(w_is_verified_genre_by_n_count).alias("first_known_stddev_debut_year"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.lag("n_rows", 1, 0).over(w_n_rows_country_by_stddev_deb).alias("prev_n_rows"),
        F.dense_rank().over(w_n_rows_country_by_stddev_deb_2).alias("stddev_debut_year_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "genre",
        "is_verified",
        "stddev_debut_year",
        "country_list",
        "median_debut_year",
        "first_known_stddev_debut_year",
        "prev_n_rows",
        "stddev_debut_year_dense_rank",
        F.size(F.col("country_list")).alias("country_list_count"),
    )
    .orderBy(
        "first_known_stddev_debut_year",
        F.col("prev_n_rows").desc_nulls_first(),
    )
)
