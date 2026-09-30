from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_country_group = Window.partitionBy("is_verified", "country_group")
w_country_group_by_debut_year = Window.partitionBy("country_group").orderBy(F.col("debut_year").desc_nulls_first())

cleaned_artists = (
    df_artists
    .filter(F.col("genre") == "Pop")
    .select(
        "id",
        "country",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

tagged = cleaned_artists.withColumn("country_clean", F.initcap(F.trim(F.col("country"))))

derived = (
    tagged
    .filter(F.col("debut_year_filled") < 2013)
    .select(
        "*",
        F.when(F.col("country").isin("JP", "KR"), "group_a").when(F.col("country") == "TH", "group_b").otherwise("other").alias("country_group"),
        F.greatest(F.col("debut_year"), F.col("debut_year_filled")).alias("greatest_debut_year_debut_year_filled"),
        F.round(F.col("debut_year_filled") / F.nullif(F.col("debut_year"), F.lit(0)), 3).alias("debut_year_filled_per_debut_year"),
    )
)

with_windows = (
    derived
    .select(
        "*",
        F.min("debut_year").over(w_is_verified_country_group).alias("is_verified_country_group_min_debut_year"),
        F.count(F.lit(1)).over(w_is_verified_country_group).alias("rows_in_is_verified_country_group"),
        F.dense_rank().over(w_country_group_by_debut_year).alias("debut_year_dense_rank"),
        F.size(F.collect_set("country").over(w_is_verified_country_group)).alias("distinct_country_in_is_verified_country_group"),
        F.sum("debut_year").over(w_is_verified_country_group).alias("is_verified_country_group_sum_debut_year"),
    )
)

per_group = (
    with_windows
    .filter(F.upper(F.trim(F.col("country_clean"))).like("T%"))
    .groupBy(
        "distinct_country_in_is_verified_country_group",
        "debut_year_dense_rank",
        "country_group",
    )
    .agg(
        F.sum(F.when(F.col("is_verified"), F.col("rows_in_is_verified_country_group"))).alias("rows_in_is_verified_country_group_is_verified_tr"),
        F.sum("debut_year_dense_rank").alias("total_debut_year_dense_rank"),
        F.sum(F.when(F.col("is_verified"), 1).otherwise(0)).alias("n_is_verified_true"),
        F.sum(F.when(F.col("country").isNotNull(), 1).otherwise(0)).alias("n_country_is_not_null"),
        F.max("rows_in_is_verified_country_group").alias("max_rows_in_is_verified_country_group_check"),
    )
    .filter(F.col("max_rows_in_is_verified_country_group_check") > 3)
    .drop("max_rows_in_is_verified_country_group_check")
)

result = (
    per_group
    .groupingSets(
        [["distinct_country_in_is_verified_country_group", "n_is_verified_true", "country_group"], ["distinct_country_in_is_verified_country_group"], []],
        "distinct_country_in_is_verified_country_group", "n_is_verified_true", "country_group",
    )
    .agg(
        F.round(F.avg("rows_in_is_verified_country_group_is_verified_tr"), 2).alias("avg_rows_in_is_verified_country_group_is_verifie"),
        F.percentile_approx("n_country_is_not_null", 0.5).alias("median_n_country_is_not_null"),
    )
    .select(
        "distinct_country_in_is_verified_country_group",
        "n_is_verified_true",
        F.coalesce(F.col("country_group"), F.lit("ALL")).alias("country_group"),
        "avg_rows_in_is_verified_country_group_is_verifie",
        "median_n_country_is_not_null",
    )
    .orderBy(
        F.col("distinct_country_in_is_verified_country_group").asc_nulls_last(),
        F.col("n_is_verified_true").asc_nulls_last(),
        F.col("country_group").asc_nulls_last(),
    )
)
