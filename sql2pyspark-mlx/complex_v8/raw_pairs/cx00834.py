from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("country", "genre").orderBy("debut_year", "id")
w_distinct_ids_genre = Window.partitionBy("distinct_ids", "genre")
w_cnt_country_by_distinct_ids = Window.partitionBy("cnt", "country").orderBy("distinct_ids", "genre", "country").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cnt_country_by_distinct_ids_2 = Window.partitionBy("cnt", "country").orderBy("distinct_ids", "genre", "country")
w_cnt_country_by_distinct_ids_3 = Window.partitionBy("cnt", "country").orderBy("distinct_ids", "genre", "country").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_artists = df_artists.filter(~F.col("country").isin("TH", "US"))

latest_per_key = (
    prepared_artists
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

rollup_base = (
    latest_per_key
    .groupBy("genre", "country")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("artist_name").isNotNull(), F.col("debut_year"))).alias("debut_year_artist_name_is_not_null"),
        F.max("debut_year").alias("max_debut_year"),
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("country").isNotNull(), 1)) / F.count("*"), 1).alias("pct_country_is_not_null"),
        F.sum("debut_year").alias("total_debut_year"),
    )
)

without_matches = (
    rollup_base
    .crossJoin(rollup_base.agg(F.max("pct_country_is_not_null").alias("overall_max_pct_country_is_not_null")))
    .filter(F.col("pct_country_is_not_null") < F.col("overall_max_pct_country_is_not_null"))
    .drop("overall_max_pct_country_is_not_null")
)

with_windows = (
    without_matches
    .select(
        "genre",
        "country",
        "debut_year_artist_name_is_not_null",
        "cnt",
        "pct_country_is_not_null",
        "total_debut_year",
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_distinct_ids_genre), 2).alias("distinct_ids_vs_distinct_ids_genre_avg"),
        F.size(F.collect_set("country").over(w_distinct_ids_genre)).alias("distinct_country_in_distinct_ids_genre"),
        F.nth_value("distinct_ids", 2).over(w_cnt_country_by_distinct_ids).alias("second_distinct_ids"),
        F.ntile(10).over(w_cnt_country_by_distinct_ids_2).alias("distinct_ids_decile"),
        F.first("cnt", ignorenulls=True).over(w_cnt_country_by_distinct_ids_3).alias("first_known_cnt"),
        F.lag("total_debut_year").over(w_cnt_country_by_distinct_ids_2).alias("prev_total_debut_year"),
    )
)

result = (
    with_windows
    .select(
        "genre",
        "country",
        "debut_year_artist_name_is_not_null",
        "cnt",
        "total_debut_year",
        "distinct_ids_vs_distinct_ids_genre_avg",
        "distinct_country_in_distinct_ids_genre",
        "second_distinct_ids",
        "distinct_ids_decile",
        "first_known_cnt",
        "prev_total_debut_year",
        (F.floor(F.col("first_known_cnt") / 2) * 2).alias("first_known_cnt_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("genre")), " ", "_")).alias("genre_slug"),
    )
    .orderBy(
        F.col("genre_slug").asc_nulls_last(),
        F.col("first_known_cnt_bucket").asc_nulls_last(),
    )
)
