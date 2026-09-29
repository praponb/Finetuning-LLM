from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_genre = Window.partitionBy("is_verified", "genre")
w_country = Window.partitionBy("country")
w_genre_genre_or_unknown_by_de = Window.partitionBy("genre", "genre_or_unknown").orderBy("debut_year", "id")
w_country_genre_or_unknown_by = Window.partitionBy("country", "genre_or_unknown").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_country_genre_or_unknown_by_2 = Window.partitionBy("country", "genre_or_unknown").orderBy("debut_year", "id")
w_all_by_debut_year_running = Window.orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_debut_year_full = Window.orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_artists
    .filter((F.col("artist_name").isNotNull()) | (F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.length(F.trim(F.col("country"))).alias("country_len"),
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
    )
)

metrics = (
    scoped
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        "genre_or_unknown",
        F.max("debut_year").over(w_is_verified_genre).alias("is_verified_genre_max_debut_year"),
        F.round(F.col("country_len") / F.nullif(F.sum("country_len").over(w_country), F.lit(0)), 4).alias("country_len_share_of_country"),
        F.size(F.collect_set("genre_or_unknown").over(w_country)).alias("distinct_genre_or_unknown_in_country"),
        F.round(F.percent_rank().over(w_genre_genre_or_unknown_by_de), 3).alias("debut_year_percent_rank"),
        F.size(F.collect_set("country").over(w_is_verified_genre)).alias("distinct_country_in_is_verified_genre"),
        F.first("country_len", ignorenulls=True).over(w_country_genre_or_unknown_by).alias("first_known_country_len"),
        F.round(F.col("country_len") - F.avg("country_len").over(w_is_verified_genre), 2).alias("country_len_vs_is_verified_genre_avg"),
        F.ntile(3).over(w_country_genre_or_unknown_by_2).alias("debut_year_tercile"),
        F.first("debut_year", ignorenulls=True).over(w_all_by_debut_year_running).alias("first_known_debut_year"),
        F.size(F.collect_set("genre").over(w_country)).alias("distinct_genre_in_country"),
        F.sum("debut_year").over(w_all_by_debut_year_running).alias("running_debut_year"),
        F.size(F.collect_set("genre_or_unknown").over(w_is_verified_genre)).alias("distinct_genre_or_unknown_in_is_verified_genre"),
        F.row_number().over(w_country_genre_or_unknown_by_2).alias("seq_no"),
        F.nth_value("debut_year", 2).over(w_all_by_debut_year_full).alias("second_debut_year"),
        F.lag("debut_year", 2, 0).over(w_country_genre_or_unknown_by_2).alias("prev2_debut_year"),
    )
)

result = (
    metrics
    .filter(F.col("first_known_country_len") <= 9)
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        "genre_or_unknown",
        "is_verified_genre_max_debut_year",
        "country_len_share_of_country",
        "distinct_genre_or_unknown_in_country",
        "distinct_country_in_is_verified_genre",
        "first_known_country_len",
        "debut_year_tercile",
        "first_known_debut_year",
        "distinct_genre_in_country",
        "distinct_genre_or_unknown_in_is_verified_genre",
        "seq_no",
        "second_debut_year",
        "prev2_debut_year",
        F.least(F.col("prev2_debut_year"), F.col("first_known_country_len")).alias("least_prev2_debut_year_first_known_country_len"),
    )
    .orderBy(F.col("first_known_debut_year").desc(), F.col("id").desc_nulls_first())
    .limit(5)
)
