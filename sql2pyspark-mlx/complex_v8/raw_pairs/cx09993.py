from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_genre_by_debut_year = Window.partitionBy("country", "genre").orderBy(F.col("debut_year").desc(), "id")
w_country_genre_by_debut_year_2 = Window.partitionBy("country", "genre").orderBy("debut_year", "id").rowsBetween(-6, Window.currentRow)
w_all_by_debut_year_ties = Window.orderBy(F.col("debut_year").desc())
w_country_genre_by_debut_year_3 = Window.partitionBy("country", "genre").orderBy(F.col("debut_year").desc())
w_country = Window.partitionBy("country")
w_country_genre_by_debut_year_4 = Window.partitionBy("country", "genre").orderBy("debut_year")
w_country_genre = Window.partitionBy("country", "genre")
w_country_genre_by_debut_year_5 = Window.partitionBy("country", "genre").orderBy("debut_year", "id")

base = (
    df_artists
    .filter(F.col("genre") == "Jazz")
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
    )
)

ranked = (
    base
    .select(
        "*",
        F.rank().over(w_country_genre_by_debut_year).alias("debut_year_rank"),
        F.sum("debut_year").over(w_country_genre_by_debut_year_2).alias("debut_year_sum_7"),
        F.rank().over(w_all_by_debut_year_ties).alias("debut_year_rank_2"),
        F.round(F.cume_dist().over(w_country_genre_by_debut_year_3), 3).alias("debut_year_cume_dist"),
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_country), 2).alias("debut_year_vs_country_avg"),
        F.dense_rank().over(w_country_genre_by_debut_year_4).alias("debut_year_dense_rank"),
        F.round(F.cume_dist().over(w_country_genre_by_debut_year_4), 3).alias("debut_year_cume_dist_2"),
        F.round(F.avg("debut_year").over(w_country), 2).alias("country_avg_debut_year"),
        F.count(F.lit(1)).over(w_country).alias("rows_in_country"),
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_country_genre), 2).alias("debut_year_vs_country_genre_avg"),
        F.lead("debut_year", 3).over(w_country_genre_by_debut_year_5).alias("next3_debut_year"),
    )
)

result = (
    ranked
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "debut_year_sum_7",
        "debut_year_rank_2",
        "debut_year_cume_dist",
        "debut_year_dense_rank",
        "country_avg_debut_year",
        "rows_in_country",
        "next3_debut_year",
    )
    .orderBy("country_avg_debut_year", F.col("debut_year_cume_dist").desc())
)
