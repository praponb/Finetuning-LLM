from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("country").orderBy(F.col("debut_year").desc(), F.col("id").desc())
w_all_by_distinct_genres_ties = Window.orderBy(F.col("distinct_genres").desc())
w_all_by_distinct_genres_full = Window.orderBy(F.col("distinct_genres").desc(), "country", "genre").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_genres_prior = Window.orderBy(F.col("distinct_genres").desc(), "country", "genre").rowsBetween(-1, -1)
w_genre = Window.partitionBy("genre")
w_all_by_distinct_genres = Window.orderBy(F.col("distinct_genres").desc(), "country", "genre")
w_distinct_ids = Window.partitionBy("distinct_ids")
w_distinct_genres_dense_rank_s = Window.partitionBy("distinct_genres_dense_rank", "second_distinct_ids").orderBy(F.col("distinct_genres_dense_rank").desc(), "country", "genre")
w_distinct_genres_dense_rank_s_2 = Window.partitionBy("distinct_genres_dense_rank", "second_distinct_ids").orderBy(F.col("distinct_genres_dense_rank").desc(), "country", "genre").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_genres_dense_rank_s_3 = Window.partitionBy("distinct_genres_dense_rank", "second_distinct_ids").orderBy(F.col("distinct_genres_dense_rank").desc())

cleaned = (
    df_artists
    .filter((~F.col("is_verified")) & (F.col("debut_year") > 1994))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        F.lower(F.regexp_replace(F.trim(F.col("artist_name")), " ", "_")).alias("artist_name_slug"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

grouped = (
    deduped
    .filter(F.col("artist_name_slug").isNotNull())
    .groupBy("country", "genre")
    .agg(
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.col("country").isin("KR", "BR"), 1)) / F.count("*"), 1).alias("pct_country_in_kr_br"),
        F.countDistinct("country").alias("distinct_countrys"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("genre").alias("distinct_genres"),
        F.sum("debut_year").alias("sum_debut_year_check"),
    )
    .filter(F.col("sum_debut_year_check") > 1997)
    .drop("sum_debut_year_check")
)

metrics = (
    grouped
    .select(
        "*",
        F.dense_rank().over(w_all_by_distinct_genres_ties).alias("distinct_genres_dense_rank"),
        F.nth_value("distinct_ids", 2).over(w_all_by_distinct_genres_full).alias("second_distinct_ids"),
        F.round(F.avg("distinct_ids").over(w_all_by_distinct_genres_prior), 2).alias("distinct_ids_avg_1_prior"),
        F.round(F.col("distinct_genres") - F.avg("distinct_genres").over(w_genre), 2).alias("distinct_genres_vs_genre_avg"),
        F.rank().over(w_all_by_distinct_genres).alias("distinct_genres_rank"),
    )
)

scored = (
    metrics
    .filter(F.col("distinct_ids") <= 9)
    .select(
        "*",
        (F.floor(F.col("pct_country_in_kr_br") / 25) * 25).alias("pct_country_in_kr_br_bucket"),
    )
)

metrics_2 = (
    scored
    .select(
        "*",
        F.round(F.avg("pct_country_in_kr_br_bucket").over(w_distinct_ids), 2).alias("distinct_ids_avg_pct_country_in_kr_br_bucket"),
        F.ntile(4).over(w_distinct_genres_dense_rank_s).alias("distinct_genres_dense_rank_quartile"),
        F.nth_value("distinct_ids", 2).over(w_distinct_genres_dense_rank_s_2).alias("second_distinct_ids_2"),
        F.round(F.col("distinct_genres_rank") / F.nullif(F.sum("distinct_genres_rank").over(w_distinct_ids), F.lit(0)), 4).alias("distinct_genres_rank_share_of_distinct_ids"),
        F.last("distinct_genres_dense_rank").over(w_distinct_genres_dense_rank_s_2).alias("last_distinct_genres_dense_rank"),
        F.round(F.cume_dist().over(w_distinct_genres_dense_rank_s_3), 3).alias("distinct_genres_dense_rank_cume_dist"),
    )
)

result = (
    metrics_2
    .select(
        "country",
        "genre",
        "row_count",
        "pct_country_in_kr_br",
        "distinct_countrys",
        "distinct_genres",
        "distinct_genres_dense_rank",
        "second_distinct_ids",
        "distinct_ids_avg_1_prior",
        "distinct_genres_vs_genre_avg",
        "distinct_genres_rank",
        "pct_country_in_kr_br_bucket",
        "second_distinct_ids_2",
        "last_distinct_genres_dense_rank",
    )
    .orderBy("distinct_countrys", F.col("pct_country_in_kr_br").desc())
)
