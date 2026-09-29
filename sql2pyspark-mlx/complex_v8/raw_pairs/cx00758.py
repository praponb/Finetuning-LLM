from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre_group_by_distinct_coun = Window.partitionBy("genre_group").orderBy(F.col("distinct_countrys").desc())
w_country_by_distinct_countrys = Window.partitionBy("country").orderBy("distinct_countrys", "genre", "is_verified", "country")
w_is_verified = Window.partitionBy("is_verified")

scoped = (
    df_artists
    .filter(F.col("debut_year") <= 2018)
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("genre")), " ", "_")).alias("genre_slug"),
    )
)

rollup_base = (
    scoped
    .filter(F.col("genre") != "Pop")
    .groupBy("genre", "is_verified", "country")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("debut_year"), 2).alias("avg_debut_year"),
        F.sum("debut_year").alias("total_debut_year"),
        F.countDistinct("country").alias("distinct_countrys"),
        F.countDistinct("genre").alias("distinct_genres"),
        F.max("debut_year").alias("max_debut_year_check"),
    )
    .filter(F.col("max_debut_year_check") > 1996)
    .drop("max_debut_year_check")
)

derived = (
    rollup_base
    .select(
        "*",
        F.coalesce(F.col("total_debut_year"), F.lit(0)).alias("total_debut_year_filled"),
        F.concat_ws(": ", F.col("genre"), F.col("country")).alias("genre_country_label"),
    )
)

scored = (
    derived
    .select(
        "*",
        F.abs(F.col("avg_debut_year") - 2003.2).alias("avg_debut_year_distance"),
        (F.floor(F.col("distinct_countrys") / 2) * 2).alias("distinct_countrys_bucket"),
        F.when(F.col("genre").isin("Pop", "Jazz"), "group_a").when(F.col("genre") == "HipHop", "group_b").otherwise("other").alias("genre_group"),
    )
)

with_windows = (
    scored
    .select(
        "genre",
        "is_verified",
        "country",
        "row_count",
        "total_debut_year",
        "distinct_genres",
        "avg_debut_year_distance",
        "distinct_countrys_bucket",
        "genre_group",
        F.rank().over(w_genre_group_by_distinct_coun).alias("distinct_countrys_rank"),
        F.row_number().over(w_country_by_distinct_countrys).alias("distinct_countrys_seq"),
        F.round(F.col("distinct_countrys_bucket") - F.avg("distinct_countrys_bucket").over(w_is_verified), 2).alias("distinct_countrys_bucket_vs_is_verified_avg"),
        F.count(F.lit(1)).over(w_is_verified).alias("rows_in_is_verified"),
    )
)

result = (
    with_windows
    .filter(F.col("row_count") <= 8)
    .select(
        "genre",
        "is_verified",
        "country",
        "row_count",
        "total_debut_year",
        "avg_debut_year_distance",
        "distinct_countrys_bucket",
        "genre_group",
        "distinct_countrys_rank",
        "distinct_countrys_bucket_vs_is_verified_avg",
        F.length(F.trim(F.col("genre_group"))).alias("genre_group_len"),
        F.lower(F.trim(F.col("country"))).alias("country_clean"),
    )
    .orderBy(F.col("distinct_countrys_bucket_vs_is_verified_avg").desc())
)
