from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_debut_year = Window.orderBy("debut_year", "id")
w_is_verified_by_debut_year_fu = Window.partitionBy("is_verified").orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_verified_by_debut_year_di = Window.partitionBy("is_verified").orderBy("debut_year_distance")
w_genre_is_verified = Window.partitionBy("genre", "is_verified")
w_is_verified_by_debut_year_di_2 = Window.partitionBy("is_verified").orderBy("debut_year_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_verified_label = Window.partitionBy("is_verified_label")
w_all_by_debut_year_running = Window.orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_verified_by_debut_year_pr = Window.partitionBy("is_verified").orderBy(F.col("debut_year").desc(), "id").rowsBetween(-1, -1)
w_is_verified_is_verified_labe = Window.partitionBy("is_verified", "is_verified_label").orderBy(F.col("debut_year_distance").desc_nulls_first(), "id")
w_debut_year_distance_quintile = Window.partitionBy("debut_year_distance_quintile")
w_n_genre_hiphop_by_n_genre_hi = Window.partitionBy("n_genre_hiphop").orderBy("n_genre_hiphop", "debut_year_distance_quintile", "genre").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_artists = (
    df_artists
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.abs(F.col("debut_year") - 1996).alias("debut_year_distance"),
        F.when(F.col("is_verified"), "yes").otherwise("no").alias("is_verified_label"),
    )
)

sequenced = (
    prepared_artists
    .select(
        "*",
        F.lead("debut_year").over(w_all_by_debut_year).alias("next_debut_year"),
        F.first("debut_year").over(w_is_verified_by_debut_year_fu).alias("first_debut_year"),
        F.round(F.cume_dist().over(w_is_verified_by_debut_year_di), 3).alias("debut_year_distance_cume_dist"),
        F.count(F.lit(1)).over(w_genre_is_verified).alias("rows_in_genre_is_verified"),
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_genre_is_verified), 2).alias("debut_year_vs_genre_is_verified_avg"),
        F.last("debut_year_distance", ignorenulls=True).over(w_is_verified_by_debut_year_di_2).alias("last_known_debut_year_distance"),
        F.size(F.collect_set("genre").over(w_is_verified_label)).alias("distinct_genre_in_is_verified_label"),
        F.last("debut_year", ignorenulls=True).over(w_all_by_debut_year_running).alias("last_known_debut_year"),
        F.sum("debut_year_distance").over(w_is_verified_by_debut_year_pr).alias("debut_year_distance_sum_1_prior"),
        F.ntile(5).over(w_is_verified_is_verified_labe).alias("debut_year_distance_quintile"),
    )
)

derived = (
    sequenced
    .filter(F.col("next_debut_year").between(1994, 2015))
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified_label",
        "next_debut_year",
        "first_debut_year",
        "debut_year_vs_genre_is_verified_avg",
        "debut_year_distance_sum_1_prior",
        "debut_year_distance_quintile",
        (F.col("last_known_debut_year") - F.col("next_debut_year")).alias("last_known_debut_year_minus_next_debut_year"),
    )
)

stats = (
    derived
    .filter(F.col("debut_year_vs_genre_is_verified_avg") <= 606.9)
    .groupBy("debut_year_distance_quintile", "genre")
    .agg(
        F.round(F.sum("debut_year_vs_genre_is_verified_avg"), 2).alias("total_debut_year_vs_genre_is_verified_avg"),
        F.round(F.stddev("debut_year_distance_sum_1_prior"), 2).alias("stddev_debut_year_distance_sum_1_prior"),
        F.array_sort(F.collect_set("country")).alias("country_list"),
        F.round(F.sum("last_known_debut_year_minus_next_debut_year") / F.nullif(F.sum("debut_year_distance_sum_1_prior"), F.lit(0)), 3).alias("last_known_debut_year_minus_next_debut_year_to_d"),
        F.sum(F.when(F.col("genre") != "HipHop", 1).otherwise(0)).alias("n_genre_hiphop"),
    )
)

with_windows = (
    stats
    .select(
        "debut_year_distance_quintile",
        "genre",
        "country_list",
        "last_known_debut_year_minus_next_debut_year_to_d",
        F.round(F.col("debut_year_distance_quintile") / F.nullif(F.sum("debut_year_distance_quintile").over(w_debut_year_distance_quintile), F.lit(0)), 4).alias("debut_year_distance_quintile_share_of_debut_year"),
        F.sum("n_genre_hiphop").over(w_n_genre_hiphop_by_n_genre_hi).alias("running_n_genre_hiphop"),
    )
)

result = (
    with_windows
    .select(
        "debut_year_distance_quintile",
        "country_list",
        "last_known_debut_year_minus_next_debut_year_to_d",
        "debut_year_distance_quintile_share_of_debut_year",
        "running_n_genre_hiphop",
        F.when(F.col("running_n_genre_hiphop") >= 70, "large").when(F.col("running_n_genre_hiphop") >= 40, "medium").otherwise("small").alias("running_n_genre_hiphop_tier"),
    )
    .orderBy(
        F.col("running_n_genre_hiphop").desc(),
        F.col("running_n_genre_hiphop_tier").asc(),
    )
)
