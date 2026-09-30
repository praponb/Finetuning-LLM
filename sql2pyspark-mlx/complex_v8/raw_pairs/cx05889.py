from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_flag = Window.partitionBy("is_explicit_flag")
w_all_by_n_is_explicit_true_fu = Window.orderBy("n_is_explicit_true", "is_explicit_flag").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_is_explicit_true_is_explic = Window.partitionBy("n_is_explicit_true", "is_explicit_flag").orderBy(F.col("distinct_artist_ids").desc(), "is_explicit_flag").rowsBetween(-3, Window.currentRow)
w_n_is_explicit_true_is_explic_2 = Window.partitionBy("n_is_explicit_true", "is_explicit_flag").orderBy(F.col("distinct_artist_ids").desc(), "is_explicit_flag")
w_all_by_n_is_explicit_true_pr = Window.orderBy("n_is_explicit_true", "is_explicit_flag").rowsBetween(-6, -1)
w_all_by_n_is_explicit_true = Window.orderBy("n_is_explicit_true", "is_explicit_flag")

scoped = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
    )
)

rollup_base = (
    scoped
    .filter(F.col("artist_id").isNotNull())
    .groupBy("is_explicit_flag")
    .agg(
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.sum(F.when(F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_true"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.min("release_date_week").alias("first_release_date_week"),
    )
)

screened = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("is_explicit_flag").alias("overall_avg_is_explicit_flag")))
    .filter(F.col("is_explicit_flag") > F.col("overall_avg_is_explicit_flag"))
    .drop("overall_avg_is_explicit_flag")
)

qualified = (
    screened
    .crossJoin(screened.agg(F.min("distinct_artist_ids").alias("overall_min_distinct_artist_ids")))
    .filter(F.col("distinct_artist_ids") > F.col("overall_min_distinct_artist_ids"))
    .drop("overall_min_distinct_artist_ids")
)

sequenced = (
    qualified
    .select(
        "is_explicit_flag",
        "first_release_date_week",
        F.round(F.col("is_explicit_flag") / F.nullif(F.sum("is_explicit_flag").over(w_is_explicit_flag), F.lit(0)), 4).alias("is_explicit_flag_share_of_is_explicit_flag"),
        F.first("distinct_artist_ids").over(w_all_by_n_is_explicit_true_fu).alias("first_distinct_artist_ids"),
        F.sum("distinct_artist_ids").over(w_n_is_explicit_true_is_explic).alias("distinct_artist_ids_sum_4"),
        F.round(F.cume_dist().over(w_n_is_explicit_true_is_explic_2), 3).alias("distinct_artist_ids_cume_dist"),
        F.max("n_is_explicit_true").over(w_all_by_n_is_explicit_true_pr).alias("n_is_explicit_true_max_6_prior"),
        F.row_number().over(w_all_by_n_is_explicit_true).alias("n_is_explicit_true_seq"),
    )
)

top_per_group = (
    sequenced
    .filter(F.col("n_is_explicit_true_seq") == 1)
    .drop("n_is_explicit_true_seq")
)

result = (
    top_per_group
    .select(
        "is_explicit_flag",
        "first_release_date_week",
        "is_explicit_flag_share_of_is_explicit_flag",
        "first_distinct_artist_ids",
        "distinct_artist_ids_sum_4",
        "distinct_artist_ids_cume_dist",
    )
    .orderBy(F.col("is_explicit_flag").desc())
)
