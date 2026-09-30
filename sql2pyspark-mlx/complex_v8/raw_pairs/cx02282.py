from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_or_unknown_genre_by = Window.partitionBy("country_or_unknown", "genre").orderBy(F.col("debut_year").asc_nulls_last(), "id")
w_is_verified_by_debut_year_ru = Window.partitionBy("is_verified").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_verified_by_debut_year = Window.partitionBy("is_verified").orderBy("debut_year", "id")
w_genre = Window.partitionBy("genre")
w_all_by_debut_year_ties = Window.orderBy("debut_year")
w_country_or_unknown_genre_by_2 = Window.partitionBy("country_or_unknown", "genre").orderBy(F.col("debut_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_verified_country_by_genre = Window.partitionBy("is_verified", "country").orderBy("genre_min_debut_year", "id")
w_is_verified_country_by_genre_2 = Window.partitionBy("is_verified", "country").orderBy("genre_min_debut_year", "id").rowsBetween(-2, Window.currentRow)
w_debut_year_decile_country_or = Window.partitionBy("debut_year_decile", "country_or_unknown")
w_all_by_rows_in_genre_ties = Window.orderBy("rows_in_genre")

filtered_artists = (
    df_artists
    .filter((~F.col("genre").isin("Pop", "Jazz")) & (~F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

metrics = (
    filtered_artists
    .select(
        "*",
        F.lag("debut_year", 1, 0).over(w_country_or_unknown_genre_by).alias("prev_debut_year"),
        F.first("debut_year", ignorenulls=True).over(w_is_verified_by_debut_year_ru).alias("first_known_debut_year"),
        F.ntile(10).over(w_is_verified_by_debut_year).alias("debut_year_decile"),
        (F.col("debut_year") - F.lag("debut_year").over(w_country_or_unknown_genre_by)).alias("debut_year_change"),
        (F.col("debut_year") - F.lag("debut_year").over(w_is_verified_by_debut_year)).alias("debut_year_change_2"),
        F.count(F.lit(1)).over(w_genre).alias("rows_in_genre"),
        F.round(F.col("debut_year") / F.nullif(F.sum("debut_year").over(w_genre), F.lit(0)), 4).alias("debut_year_share_of_genre"),
        F.round(F.percent_rank().over(w_all_by_debut_year_ties), 3).alias("debut_year_percent_rank"),
        F.min("debut_year").over(w_genre).alias("genre_min_debut_year"),
        F.sum("debut_year").over(w_country_or_unknown_genre_by_2).alias("running_debut_year"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.lead("debut_year_change", 2).over(w_is_verified_country_by_genre).alias("next2_debut_year_change"),
        F.round(F.avg("rows_in_genre").over(w_is_verified_country_by_genre_2), 2).alias("rows_in_genre_avg_3"),
        F.sum("running_debut_year").over(w_debut_year_decile_country_or).alias("debut_year_decile_country_or_unknown_sum_running"),
        F.ntile(3).over(w_is_verified_country_by_genre).alias("genre_min_debut_year_tercile"),
        F.round(F.percent_rank().over(w_all_by_rows_in_genre_ties), 3).alias("rows_in_genre_percent_rank"),
        F.lag("running_debut_year").over(w_is_verified_country_by_genre).alias("prev_running_debut_year"),
        F.count(F.lit(1)).over(w_debut_year_decile_country_or).alias("rows_in_debut_year_decile_country_or_unknown"),
        F.dense_rank().over(w_all_by_rows_in_genre_ties).alias("rows_in_genre_dense_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("rows_in_genre_avg_3") >= 3.9)
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        "country_or_unknown",
        "prev_debut_year",
        "first_known_debut_year",
        "debut_year_decile",
        "debut_year_change",
        "debut_year_change_2",
        "rows_in_genre",
        "debut_year_share_of_genre",
        "genre_min_debut_year",
        "running_debut_year",
        "next2_debut_year_change",
        "debut_year_decile_country_or_unknown_sum_running",
        "rows_in_genre_percent_rank",
        "prev_running_debut_year",
        "rows_in_genre_dense_rank",
    )
    .orderBy(F.col("next2_debut_year_change").desc())
)
