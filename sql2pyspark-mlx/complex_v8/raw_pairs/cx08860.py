from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_debut_year_moving_3 = Window.orderBy(F.col("debut_year").desc(), "id").rowsBetween(-3, Window.currentRow)
w_is_verified_genre_group_by_d = Window.partitionBy("is_verified", "genre_group").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_genre_group_by_debut_year_fu = Window.partitionBy("genre_group").orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_verified_genre_group_by_d_2 = Window.partitionBy("is_verified", "genre_group").orderBy("debut_year")
w_genre_group_by_debut_year = Window.partitionBy("genre_group").orderBy(F.col("debut_year").desc(), "id")

cleaned_artists = (
    df_artists
    .filter((F.col("genre").isNotNull()) & (F.length(F.trim(F.col("artist_name"))) >= 7))
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("genre").isin("Jazz", "Indie"), "group_a").when(F.col("genre") == "Pop", "group_b").otherwise("other").alias("genre_group"),
    )
)

sequenced = (
    cleaned_artists
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        F.round(F.avg("debut_year").over(w_all_by_debut_year_moving_3), 2).alias("debut_year_avg_4"),
        F.first("debut_year", ignorenulls=True).over(w_is_verified_genre_group_by_d).alias("first_known_debut_year"),
        F.nth_value("debut_year", 2).over(w_genre_group_by_debut_year_fu).alias("second_debut_year"),
        F.round(F.cume_dist().over(w_is_verified_genre_group_by_d_2), 3).alias("debut_year_cume_dist"),
        F.round(F.percent_rank().over(w_genre_group_by_debut_year), 3).alias("debut_year_percent_rank"),
    )
)

result = (
    sequenced
    .rollup("country", "is_verified")
    .agg(
        F.grouping("country").alias("is_total_country"),
        F.max("second_debut_year").alias("max_second_debut_year"),
        F.array_sort(F.collect_set("country")).alias("country_list"),
        F.sum(F.when(F.col("country") != "BR", F.col("second_debut_year")).otherwise(0)).alias("second_debut_year_country_br"),
    )
    .orderBy(
        F.col("country").asc_nulls_last(),
        F.col("is_verified").asc_nulls_last(),
    )
)
