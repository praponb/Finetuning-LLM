from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_debut_year = Window.orderBy(F.col("debut_year").desc(), "id")
w_is_verified_debut_year_tier = Window.partitionBy("is_verified", "debut_year_tier").orderBy("debut_year", "id")
w_is_verified_country = Window.partitionBy("is_verified", "country")
w_genre_by_debut_year_running = Window.partitionBy("genre").orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_debut_year_ties = Window.orderBy("debut_year")
w_genre_by_debut_year = Window.partitionBy("genre").orderBy(F.col("debut_year").desc_nulls_first(), "id")
w_all_by_debut_year_2 = Window.orderBy("debut_year", "id")
w_is_verified_debut_year_tier_2 = Window.partitionBy("is_verified", "debut_year_tier").orderBy("debut_year", "id").rowsBetween(-6, Window.currentRow)
w_debut_year_quintile_distinct = Window.partitionBy("debut_year_quintile", "distinct_genre_in_is_verified_country").orderBy(F.col("is_verified_country_max_debut_year").asc_nulls_last(), "id")
w_distinct_genre_in_is_verifie = Window.partitionBy("distinct_genre_in_is_verified_country", "is_verified").orderBy("debut_year_dense_rank_minus_last_known_debut_yea", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_debut_year_quintile_distinct_2 = Window.partitionBy("debut_year_quintile", "distinct_genre_in_is_verified_country").orderBy(F.col("is_verified_country_max_debut_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_verified = Window.partitionBy("is_verified")

recent = (
    df_artists
    .filter((F.length(F.trim(F.col("artist_name"))) >= 7) & (F.col("genre").isin("HipHop", "Indie")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("debut_year") >= 2018, "HIGH").when(F.col("debut_year") >= 1996, "MEDIUM").otherwise("LOW").alias("debut_year_tier"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.dense_rank().over(w_all_by_debut_year).alias("debut_year_dense_rank"),
        F.ntile(5).over(w_is_verified_debut_year_tier).alias("debut_year_quintile"),
        F.count(F.lit(1)).over(w_is_verified_country).alias("rows_in_is_verified_country"),
        F.last("debut_year", ignorenulls=True).over(w_genre_by_debut_year_running).alias("last_known_debut_year"),
        F.size(F.collect_set("genre").over(w_is_verified_country)).alias("distinct_genre_in_is_verified_country"),
        F.rank().over(w_all_by_debut_year_ties).alias("debut_year_rank"),
        F.lead("debut_year").over(w_genre_by_debut_year).alias("next_debut_year"),
        F.max("debut_year").over(w_is_verified_country).alias("is_verified_country_max_debut_year"),
        F.lag("debut_year", 2).over(w_genre_by_debut_year).alias("prev2_debut_year"),
        F.lag("debut_year").over(w_all_by_debut_year_2).alias("prev_debut_year"),
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_is_verified_country), F.lit(0)), 4).alias("debut_year_share_of_is_verified_country"),
        F.round(F.avg("debut_year").over(w_is_verified_debut_year_tier_2), 2).alias("debut_year_avg_7"),
        F.sum("debut_year").over(w_is_verified_country).alias("is_verified_country_sum_debut_year"),
        (F.col("debut_year") - F.lag("debut_year").over(w_all_by_debut_year)).alias("debut_year_change"),
    )
)

enriched = (
    ranked
    .filter(F.col("debut_year_rank") > 4)
    .select(
        "id",
        "artist_name",
        "country",
        "is_verified",
        "debut_year_dense_rank",
        "debut_year_quintile",
        "last_known_debut_year",
        "distinct_genre_in_is_verified_country",
        "debut_year_rank",
        "next_debut_year",
        "is_verified_country_max_debut_year",
        "prev_debut_year",
        "debut_year_avg_7",
        "is_verified_country_sum_debut_year",
        "debut_year_change",
        F.round(F.col("debut_year_rank") / F.nullif(F.col("debut_year_change"), F.lit(0)), 3).alias("debut_year_rank_per_debut_year_change"),
        (F.col("debut_year_dense_rank") - F.col("last_known_debut_year")).alias("debut_year_dense_rank_minus_last_known_debut_yea"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "debut_year_quintile",
        "debut_year_rank",
        "next_debut_year",
        "prev_debut_year",
        "debut_year_avg_7",
        "debut_year_change",
        "debut_year_dense_rank_minus_last_known_debut_yea",
        F.round(F.cume_dist().over(w_debut_year_quintile_distinct), 3).alias("is_verified_country_max_debut_year_cume_dist"),
        F.first("debut_year_change").over(w_distinct_genre_in_is_verifie).alias("first_debut_year_change"),
        F.nth_value("debut_year_avg_7", 2).over(w_debut_year_quintile_distinct_2).alias("second_debut_year_avg_7"),
        (F.col("debut_year_avg_7") - F.lag("debut_year_avg_7").over(w_debut_year_quintile_distinct)).alias("debut_year_avg_7_change"),
        F.round(F.avg("debut_year_change").over(w_is_verified), 2).alias("is_verified_avg_debut_year_change"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "debut_year_rank",
        "next_debut_year",
        "prev_debut_year",
        "debut_year_avg_7",
        "debut_year_change",
        "debut_year_dense_rank_minus_last_known_debut_yea",
        "is_verified_country_max_debut_year_cume_dist",
        "first_debut_year_change",
        "second_debut_year_avg_7",
        "is_verified_avg_debut_year_change",
    )
    .orderBy(
        F.col("next_debut_year").asc_nulls_last(),
        F.col("second_debut_year_avg_7").asc_nulls_last(),
    )
)
