from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_by_debut_year_runnin = Window.partitionBy("country").orderBy(F.col("debut_year").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_country_by_debut_year = Window.partitionBy("country").orderBy(F.col("debut_year").desc_nulls_first(), "id")
w_genre_by_debut_year_prior_2 = Window.partitionBy("genre").orderBy(F.col("debut_year").desc(), "id").rowsBetween(-2, -1)
w_all_by_debut_year_moving_6 = Window.orderBy("debut_year", "id").rowsBetween(-6, Window.currentRow)
w_genre_is_verified = Window.partitionBy("genre", "is_verified")
w_country = Window.partitionBy("country")
w_all_by_debut_year_full = Window.orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_genre_by_debut_year = Window.partitionBy("genre").orderBy(F.col("debut_year").desc(), "id")
w_country_by_debut_year_ties = Window.partitionBy("country").orderBy(F.col("debut_year").desc_nulls_first())

base_artists = df_artists.filter(F.col("artist_name").isNotNull())

with_windows = (
    base_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.last("debut_year", ignorenulls=True).over(w_country_by_debut_year_runnin).alias("last_known_debut_year"),
        F.rank().over(w_country_by_debut_year).alias("debut_year_rank"),
        F.sum("debut_year").over(w_genre_by_debut_year_prior_2).alias("debut_year_sum_2_prior"),
        F.first("debut_year", ignorenulls=True).over(w_country_by_debut_year_runnin).alias("first_known_debut_year"),
        F.round(F.avg("debut_year").over(w_all_by_debut_year_moving_6), 2).alias("debut_year_avg_7"),
        F.sum("debut_year").over(w_genre_is_verified).alias("genre_is_verified_sum_debut_year"),
        F.round(F.avg("debut_year").over(w_country), 2).alias("country_avg_debut_year"),
        F.nth_value("debut_year", 2).over(w_all_by_debut_year_full).alias("second_debut_year"),
        F.ntile(5).over(w_genre_by_debut_year).alias("debut_year_quintile"),
        F.count(F.lit(1)).over(w_country).alias("rows_in_country"),
        F.round(F.cume_dist().over(w_country_by_debut_year_ties), 3).alias("debut_year_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_name",
        "debut_year",
        "is_verified",
        "last_known_debut_year",
        "debut_year_rank",
        "debut_year_sum_2_prior",
        "first_known_debut_year",
        "debut_year_avg_7",
        "country_avg_debut_year",
        "debut_year_cume_dist",
        F.when(F.col("debut_year_cume_dist") >= 0.6, "A").when(F.col("debut_year_cume_dist") >= 0.3, "B").otherwise("C").alias("debut_year_cume_dist_tier"),
        F.length(F.trim(F.col("genre"))).alias("genre_len"),
    )
    .orderBy("debut_year", "id")
    .limit(10)
)
