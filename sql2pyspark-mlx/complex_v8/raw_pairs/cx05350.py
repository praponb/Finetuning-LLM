from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_genre_by_debut_year = Window.partitionBy("country", "genre").orderBy(F.col("debut_year").desc())
w_genre = Window.partitionBy("genre")
w_country_genre = Window.partitionBy("country", "genre")
w_country_by_debut_year = Window.partitionBy("country").orderBy("debut_year", "id")
w_genre_country_by_debut_year = Window.partitionBy("genre", "country").orderBy("debut_year", "id")
w_country_genre_by_debut_year_2 = Window.partitionBy("country", "genre").orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_debut_year = Window.orderBy(F.col("debut_year").desc_nulls_first(), "id")
w_country_genre_by_debut_year_3 = Window.partitionBy("country", "genre").orderBy(F.col("debut_year").desc(), "id")
w_debut_year_rank_2_genre_by_d = Window.partitionBy("debut_year_rank_2", "genre").orderBy("debut_year_rank_2", "id")
w_country_debut_year_rank_2_by = Window.partitionBy("country", "debut_year_rank_2").orderBy("next2_debut_year", "id")
w_debut_year_rank_by_genre_avg = Window.partitionBy("debut_year_rank").orderBy(F.col("genre_avg_debut_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_artists = (
    df_artists
    .filter((F.col("country") == "US") & (F.col("genre") == "Classical"))
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
    )
)

sequenced = (
    prepared_artists
    .select(
        "*",
        F.rank().over(w_country_genre_by_debut_year).alias("debut_year_rank"),
        F.count(F.lit(1)).over(w_genre).alias("rows_in_genre"),
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_country_genre), F.lit(0)), 4).alias("debut_year_share_of_country_genre"),
        F.round(F.avg("debut_year").over(w_genre), 2).alias("genre_avg_debut_year"),
        F.lag("debut_year").over(w_country_by_debut_year).alias("prev_debut_year"),
        F.round(F.percent_rank().over(w_genre_country_by_debut_year), 3).alias("debut_year_percent_rank"),
        F.first("debut_year", ignorenulls=True).over(w_country_genre_by_debut_year_2).alias("first_known_debut_year"),
        F.rank().over(w_all_by_debut_year).alias("debut_year_rank_2"),
        F.lead("debut_year", 2).over(w_country_genre_by_debut_year_3).alias("next2_debut_year"),
    )
)

top_per_group = sequenced.filter(F.col("debut_year_rank") == 1)

ranked = (
    top_per_group
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "debut_year_rank",
        "rows_in_genre",
        "genre_avg_debut_year",
        "prev_debut_year",
        "debut_year_percent_rank",
        "first_known_debut_year",
        "debut_year_rank_2",
        "next2_debut_year",
        F.lead("first_known_debut_year", 2, 0).over(w_debut_year_rank_2_genre_by_d).alias("next2_first_known_debut_year"),
        (F.col("prev_debut_year") - F.lag("prev_debut_year").over(w_country_debut_year_rank_2_by)).alias("prev_debut_year_change"),
        F.last("prev_debut_year", ignorenulls=True).over(w_debut_year_rank_by_genre_avg).alias("last_known_prev_debut_year"),
    )
)

result = (
    ranked
    .filter(F.col("prev_debut_year_change") < 1214)
    .select(
        "id",
        "genre",
        "debut_year_rank",
        "rows_in_genre",
        "genre_avg_debut_year",
        "prev_debut_year",
        "debut_year_percent_rank",
        "debut_year_rank_2",
        "next2_debut_year",
        "next2_first_known_debut_year",
        "last_known_prev_debut_year",
        F.initcap(F.trim(F.col("genre"))).alias("genre_clean"),
    )
    .orderBy("debut_year_rank", F.col("debut_year_percent_rank").desc_nulls_first())
)
