from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_release_date = Window.orderBy("release_date", "id")
w_all_by_release_date_year = Window.orderBy(F.col("release_date_year").desc_nulls_first(), "id")
w_release_date_year_is_explici = Window.partitionBy("release_date_year", "is_explicit").orderBy("release_date_year")
w_is_explicit_release_date_yea = Window.partitionBy("is_explicit", "release_date_year")

filtered = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-06-03").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.year(F.col("release_date")).alias("release_date_year"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "release_date_year",
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.sum("release_date_year").over(w_is_explicit).alias("is_explicit_sum_release_date_year"),
        F.min("release_date_year").over(w_is_explicit).alias("is_explicit_min_release_date_year"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.lead("release_date_year", 2).over(w_all_by_release_date).alias("next2_release_date_year"),
        F.round(F.col("release_date_year") / F.nullif(F.sum("release_date_year").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("release_date_year_share_of_artist_id_is_explicit"),
        F.row_number().over(w_all_by_release_date_year).alias("release_date_year_seq"),
        F.round(F.cume_dist().over(w_release_date_year_is_explici), 3).alias("release_date_year_cume_dist"),
    )
)

top_rows = with_windows.filter(F.col("release_date_year_seq") == 1)

totals = (
    top_rows
    .filter(F.col("is_explicit_min_release_date_year") > 9318)
    .groupBy("release_date_year", "is_explicit", "next2_release_date_year")
    .agg(
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.max("release_date").alias("last_release_date"),
        F.sum("is_explicit_sum_release_date_year").alias("total_is_explicit_sum_release_date_year"),
    )
)

with_windows_2 = (
    totals
    .withColumn("rows_in_is_explicit_release_date_year", F.count(F.lit(1)).over(w_is_explicit_release_date_yea))
    .withColumn("is_explicit_release_date_year_min_release_date_y", F.min("release_date_year").over(w_is_explicit_release_date_yea))
)

result = (
    with_windows_2
    .filter(F.col("next2_release_date_year") < 2025)
    .select(
        "release_date_year",
        "is_explicit",
        "title_list",
        "last_release_date",
        "total_is_explicit_sum_release_date_year",
    )
    .orderBy(
        F.col("release_date_year").desc_nulls_first(),
        F.col("last_release_date").asc_nulls_last(),
    )
)
