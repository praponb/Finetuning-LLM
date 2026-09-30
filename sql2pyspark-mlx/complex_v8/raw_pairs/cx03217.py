from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_by_debut_year_distan = Window.partitionBy("country").orderBy(F.col("debut_year_distance").desc_nulls_first(), "id")
w_country_genre = Window.partitionBy("country", "genre")
w_country_by_debut_year_distan_2 = Window.partitionBy("country").orderBy(F.col("debut_year_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_artists
    .filter((~F.col("is_verified")) & (F.col("debut_year") > 1997))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "is_verified",
        F.concat(F.col("genre"), F.lit("#"), F.col("debut_year").cast("string")).alias("genre_ref"),
        F.abs(F.col("debut_year") - 2006).alias("debut_year_distance"),
    )
)

windowed = (
    base
    .select(
        "*",
        (F.col("debut_year_distance") - F.lag("debut_year_distance").over(w_country_by_debut_year_distan)).alias("debut_year_distance_change"),
        F.min("debut_year_distance").over(w_country_genre).alias("country_genre_min_debut_year_distance"),
        F.first("debut_year_distance", ignorenulls=True).over(w_country_by_debut_year_distan_2).alias("first_known_debut_year_distance"),
        F.ntile(10).over(w_country_by_debut_year_distan).alias("debut_year_distance_decile"),
    )
)

stats = (
    windowed
    .groupBy("genre", "country")
    .agg(
        F.sum("debut_year_distance").alias("total_debut_year_distance"),
        F.round(F.sum("country_genre_min_debut_year_distance") / F.nullif(F.sum("debut_year_distance_decile"), F.lit(0)), 3).alias("country_genre_min_debut_year_distance_to_debut_y"),
        F.array_sort(F.collect_set("genre")).alias("genre_list"),
        F.count(F.when(F.upper(F.trim(F.col("artist_name"))).like("L%"), 1)).alias("n_upper_trim_artist_name_like"),
        F.avg("debut_year_distance_decile").alias("avg_debut_year_distance_decile_check"),
    )
    .filter(F.col("avg_debut_year_distance_decile_check") > 3)
    .drop("avg_debut_year_distance_decile_check")
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.avg("country_genre_min_debut_year_distance_to_debut_y").alias("overall_avg_country_genre_min_debut_year_distanc")))
    .filter(F.col("country_genre_min_debut_year_distance_to_debut_y") > F.col("overall_avg_country_genre_min_debut_year_distanc"))
    .drop("overall_avg_country_genre_min_debut_year_distanc")
)

qualified = (
    without_matches
    .crossJoin(without_matches.agg(F.avg("country_genre_min_debut_year_distance_to_debut_y").alias("overall_avg_country_genre_min_debut_year_distanc")))
    .filter(F.col("country_genre_min_debut_year_distance_to_debut_y") > F.col("overall_avg_country_genre_min_debut_year_distanc"))
    .drop("overall_avg_country_genre_min_debut_year_distanc")
)

result = (
    qualified
    .groupingSets(
        [["n_upper_trim_artist_name_like", "genre"], ["n_upper_trim_artist_name_like"]],
        "n_upper_trim_artist_name_like", "genre",
    )
    .agg(
        F.round(F.avg("n_upper_trim_artist_name_like"), 2).alias("avg_n_upper_trim_artist_name_like"),
    )
    .orderBy(
        F.col("n_upper_trim_artist_name_like").asc_nulls_last(),
        F.col("genre").asc_nulls_last(),
    )
)
