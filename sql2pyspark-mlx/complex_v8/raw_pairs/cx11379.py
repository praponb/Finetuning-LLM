from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre_country_by_debut_year = Window.partitionBy("genre", "country").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_verified = Window.partitionBy("is_verified")
w_genre_country_by_debut_year_2 = Window.partitionBy("genre", "country").orderBy("debut_year", "id").rowsBetween(-6, -1)
w_genre_by_debut_year = Window.partitionBy("genre").orderBy("debut_year", "id")
w_country_genre = Window.partitionBy("country", "genre")
w_is_verified_genre_by_debut_y = Window.partitionBy("is_verified", "genre").orderBy("debut_year", "id")
w_all_by_debut_year_bucket = Window.orderBy("debut_year_bucket", "id")
w_is_verified_genre_by_debut_y_2 = Window.partitionBy("is_verified", "genre").orderBy("debut_year")

scoped_artists = (
    df_artists
    .filter(F.lower(F.col("artist_name")).like("%ar%"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        (F.floor(F.col("debut_year") / 5) * 5).alias("debut_year_bucket"),
    )
)

sequenced = (
    scoped_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        F.sum("debut_year").over(w_genre_country_by_debut_year).alias("running_debut_year"),
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_is_verified), F.lit(0)), 4).alias("debut_year_share_of_is_verified"),
        F.sum("debut_year").over(w_genre_country_by_debut_year_2).alias("debut_year_sum_6_prior"),
        F.count(F.lit(1)).over(w_is_verified).alias("rows_in_is_verified"),
        F.row_number().over(w_genre_by_debut_year).alias("debut_year_seq"),
        F.round(F.avg("debut_year_bucket").over(w_is_verified), 2).alias("is_verified_avg_debut_year_bucket"),
        F.min("debut_year").over(w_country_genre).alias("country_genre_min_debut_year"),
        F.row_number().over(w_is_verified_genre_by_debut_y).alias("rn"),
        F.lead("debut_year_bucket", 3).over(w_all_by_debut_year_bucket).alias("next3_debut_year_bucket"),
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_is_verified), 2).alias("debut_year_vs_is_verified_avg"),
        F.round(F.cume_dist().over(w_is_verified_genre_by_debut_y_2), 3).alias("debut_year_cume_dist"),
    )
)

leaders = (
    sequenced
    .filter((F.col("rn") <= 5) & (F.col("debut_year_seq").between(4, 9)))
    .drop("rn")
)

result = (
    leaders
    .filter(F.col("next3_debut_year_bucket") > 1993)
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "running_debut_year",
        "debut_year_share_of_is_verified",
        "rows_in_is_verified",
        "debut_year_seq",
        "is_verified_avg_debut_year_bucket",
        "next3_debut_year_bucket",
        "debut_year_vs_is_verified_avg",
        "debut_year_cume_dist",
    )
    .orderBy(F.col("artist_name").desc(), F.col("running_debut_year").desc())
)
