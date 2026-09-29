from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_debut_year_running = Window.orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_debut_year_running_2 = Window.orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_genre_by_debut_year = Window.partitionBy("genre").orderBy(F.col("debut_year").desc(), "id")
w_genre_group_by_debut_year = Window.partitionBy("genre_group").orderBy("debut_year", "id")
w_genre = Window.partitionBy("genre")
w_is_verified_genre_group_by_d = Window.partitionBy("is_verified", "genre_group").orderBy("debut_year", "id")
w_genre_group_by_debut_year_pr = Window.partitionBy("genre_group").orderBy("debut_year", "id").rowsBetween(-3, -1)
w_genre_by_debut_year_running = Window.partitionBy("genre").orderBy(F.col("debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_debut_year_full = Window.orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_verified_genre = Window.partitionBy("is_verified", "genre")
w_genre_group_is_verified = Window.partitionBy("genre_group", "is_verified")
w_distinct_genre_group_in_is_v = Window.partitionBy("distinct_genre_group_in_is_verified_genre").orderBy("debut_year_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_genre_group_in_is_v_2 = Window.partitionBy("distinct_genre_group_in_is_verified_genre").orderBy("debut_year_avg_3_prior", "id")
w_distinct_genre_group_in_is_v_3 = Window.partitionBy("distinct_genre_group_in_is_verified_genre").orderBy("debut_year_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_artists
    .filter((F.col("debut_year") < 2020) | (F.col("country") == "KR"))
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        "is_verified",
        F.when(F.col("genre").isin("Jazz", "Pop"), "group_a").when(F.col("genre") == "Indie", "group_b").otherwise("other").alias("genre_group"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "genre",
        "debut_year",
        "is_verified",
        "genre_group",
        F.last("debut_year", ignorenulls=True).over(w_all_by_debut_year_running).alias("last_known_debut_year"),
        F.sum("debut_year").over(w_all_by_debut_year_running_2).alias("running_debut_year"),
        F.rank().over(w_genre_by_debut_year).alias("debut_year_rank"),
        F.lag("debut_year").over(w_genre_group_by_debut_year).alias("prev_debut_year"),
        F.max("debut_year").over(w_genre).alias("genre_max_debut_year"),
        F.round(F.percent_rank().over(w_is_verified_genre_group_by_d), 3).alias("debut_year_percent_rank"),
        F.round(F.avg("debut_year").over(w_genre_group_by_debut_year_pr), 2).alias("debut_year_avg_3_prior"),
        F.last("debut_year", ignorenulls=True).over(w_genre_by_debut_year_running).alias("last_known_debut_year_2"),
        F.last("debut_year").over(w_all_by_debut_year_full).alias("last_debut_year"),
        F.count(F.lit(1)).over(w_is_verified_genre).alias("rows_in_is_verified_genre"),
        F.size(F.collect_set("genre_group").over(w_is_verified_genre)).alias("distinct_genre_group_in_is_verified_genre"),
    )
)

first_rows = (
    with_trends
    .filter(F.col("debut_year_rank") <= 3)
    .drop("debut_year_rank")
)

with_windows = (
    first_rows
    .select(
        "id",
        "genre",
        "debut_year",
        "is_verified",
        "genre_group",
        "running_debut_year",
        "prev_debut_year",
        "genre_max_debut_year",
        "last_known_debut_year_2",
        "last_debut_year",
        "rows_in_is_verified_genre",
        F.count(F.lit(1)).over(w_genre_group_is_verified).alias("rows_in_genre_group_is_verified"),
        F.min("debut_year_avg_3_prior").over(w_genre_group_is_verified).alias("genre_group_is_verified_min_debut_year_avg_3_pri"),
        F.max("last_known_debut_year_2").over(w_distinct_genre_group_in_is_v).alias("max_last_known_debut_year_2_so_far"),
        F.round(F.avg("last_known_debut_year_2").over(w_genre_group_is_verified), 2).alias("genre_group_is_verified_avg_last_known_debut_yea"),
        F.ntile(3).over(w_distinct_genre_group_in_is_v_2).alias("debut_year_avg_3_prior_tercile"),
        F.nth_value("running_debut_year", 2).over(w_distinct_genre_group_in_is_v_3).alias("second_running_debut_year"),
        F.max("rows_in_is_verified_genre").over(w_genre_group_is_verified).alias("genre_group_is_verified_max_rows_in_is_verified_"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "debut_year",
        "is_verified",
        "genre_group",
        "running_debut_year",
        "prev_debut_year",
        "genre_max_debut_year",
        "last_known_debut_year_2",
        "rows_in_is_verified_genre",
        "rows_in_genre_group_is_verified",
        "genre_group_is_verified_min_debut_year_avg_3_pri",
        "max_last_known_debut_year_2_so_far",
        "genre_group_is_verified_avg_last_known_debut_yea",
        "second_running_debut_year",
        "genre_group_is_verified_max_rows_in_is_verified_",
    )
    .orderBy(F.col("id").desc(), F.col("running_debut_year").desc())
)
