from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_gre = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("greatest_duration_sec_bpm").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_release_date_month_month_art = Window.partitionBy(F.date_trunc("month", "release_date_month"), "artist_id")
w_release_date_year_is_explici = Window.partitionBy(F.year("release_date"), "is_explicit")
w_release_date_month_month_is = Window.partitionBy(F.month("release_date_month"), "is_explicit").orderBy(F.col("release_date_month").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit_by_gre_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("greatest_duration_sec_bpm").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_release_date_month_month_is_2 = Window.partitionBy(F.month("release_date_month"), "is_explicit").orderBy(F.col("release_date_month").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_release_date_month_is_explic = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit")
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy("release_date", "id").rowsBetween(-3, Window.currentRow)
w_second_release_date_month_to = Window.partitionBy(F.to_date("second_release_date_month"), "is_explicit")
w_all_by_release_date_month = Window.orderBy("release_date_month", "id")
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.year("release_date_month")).orderBy("days_since_prev_release_date", "id")
w_release_date_month_distinct = Window.partitionBy(F.date_trunc("month", "release_date"), "distinct_artist_id_in_release_date_month_is_expl").orderBy("release_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_month_2 = Window.partitionBy("artist_id", F.year("release_date_month")).orderBy("days_since_prev_release_date")
w_n_not_is_explicit_release_da = Window.partitionBy("n_not_is_explicit", "release_date_month_quintile").orderBy(F.col("min_greatest_duration_sec_bpm_max_6_prior").desc(), "release_date_month_quintile", "first_known_release_date_month_day").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_not_is_explicit_release_da_2 = Window.partitionBy("n_not_is_explicit", "release_date_month_quintile").orderBy(F.col("min_greatest_duration_sec_bpm_max_6_prior").desc(), "release_date_month_quintile", "first_known_release_date_month_day")

filtered = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("G%"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.max("greatest_duration_sec_bpm").over(w_artist_id_is_explicit_by_gre).alias("greatest_duration_sec_bpm_max_6_prior"),
        F.round(F.col("greatest_duration_sec_bpm") / F.nullif(F.sum("greatest_duration_sec_bpm").over(w_release_date_month_month_art), F.lit(0)), 4).alias("greatest_duration_sec_bpm_share_of_release_date_"),
        F.round(F.avg("greatest_duration_sec_bpm").over(w_release_date_year_is_explici), 2).alias("release_date_year_is_explicit_avg_greatest_durat"),
        F.first("release_date_month", ignorenulls=True).over(w_release_date_month_month_is).alias("first_known_release_date_month"),
        F.round(F.avg("greatest_duration_sec_bpm").over(w_release_date_month_month_art), 2).alias("release_date_month_month_artist_id_avg_greatest_"),
        F.round(F.avg("greatest_duration_sec_bpm").over(w_artist_id_is_explicit_by_gre_2), 2).alias("greatest_duration_sec_bpm_avg_3"),
        F.count(F.lit(1)).over(w_release_date_month_month_art).alias("rows_in_release_date_month_month_artist_id"),
        F.nth_value("release_date_month", 2).over(w_release_date_month_month_is_2).alias("second_release_date_month"),
        F.ntile(10).over(w_is_explicit_by_release_date).alias("release_date_decile"),
        F.round(F.col("greatest_duration_sec_bpm") - F.avg("greatest_duration_sec_bpm").over(w_release_date_month_is_explic), 2).alias("greatest_duration_sec_bpm_vs_release_date_month_"),
        F.size(F.collect_set("artist_id").over(w_release_date_month_is_explic)).alias("distinct_artist_id_in_release_date_month_is_expl"),
        F.round(F.avg("greatest_duration_sec_bpm").over(w_is_explicit_artist_id_by_rel), 2).alias("greatest_duration_sec_bpm_avg_4"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_by_release_date)).alias("days_since_prev_release_date"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        "release_date_month",
        "greatest_duration_sec_bpm_max_6_prior",
        "greatest_duration_sec_bpm_share_of_release_date_",
        "release_date_year_is_explicit_avg_greatest_durat",
        "first_known_release_date_month",
        "release_date_month_month_artist_id_avg_greatest_",
        "greatest_duration_sec_bpm_avg_3",
        "second_release_date_month",
        "greatest_duration_sec_bpm_vs_release_date_month_",
        "distinct_artist_id_in_release_date_month_is_expl",
        "greatest_duration_sec_bpm_avg_4",
        F.round(F.avg("days_since_prev_release_date").over(w_second_release_date_month_to), 2).alias("second_release_date_month_to_date_is_explicit_av"),
        F.lag("release_date_decile", 3).over(w_all_by_release_date_month).alias("prev3_release_date_decile"),
        F.dense_rank().over(w_artist_id_release_date_month).alias("days_since_prev_release_date_dense_rank"),
        F.last("second_release_date_month").over(w_release_date_month_distinct).alias("last_second_release_date_month"),
        F.dense_rank().over(w_artist_id_release_date_month_2).alias("days_since_prev_release_date_dense_rank_2"),
        F.ntile(5).over(w_all_by_release_date_month).alias("release_date_month_quintile"),
    )
)

rollup_base = (
    metrics
    .groupBy(
        "release_date_month_quintile",
        F.to_date("first_known_release_date_month").alias("first_known_release_date_month_day"),
    )
    .agg(
        F.round(100 * F.count(F.when(~F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_not_is_explicit"),
        F.sum(F.when(~F.col("is_explicit"), F.col("prev3_release_date_decile"))).alias("prev3_release_date_decile_is_explicit_false"),
        F.min("greatest_duration_sec_bpm_max_6_prior").alias("min_greatest_duration_sec_bpm_max_6_prior"),
        F.round(F.sum("release_date_month_month_artist_id_avg_greatest_"), 2).alias("total_release_date_month_month_artist_id_avg_gre"),
        F.sum(F.when(~F.col("is_explicit"), 1).otherwise(0)).alias("n_not_is_explicit"),
        F.avg("distinct_artist_id_in_release_date_month_is_expl").alias("avg_distinct_artist_id_in_release_date_month_is_"),
    )
    .filter(F.col("avg_distinct_artist_id_in_release_date_month_is_") > 2)
    .drop("avg_distinct_artist_id_in_release_date_month_is_")
)

with_windows_2 = (
    rollup_base
    .select(
        "*",
        F.sum("n_not_is_explicit").over(w_n_not_is_explicit_release_da).alias("running_n_not_is_explicit"),
        F.min("release_date_month_quintile").over(w_n_not_is_explicit_release_da).alias("min_release_date_month_quintile_so_far"),
        F.rank().over(w_n_not_is_explicit_release_da_2).alias("min_greatest_duration_sec_bpm_max_6_prior_rank"),
        (F.col("pct_not_is_explicit") - F.lag("pct_not_is_explicit").over(w_n_not_is_explicit_release_da_2)).alias("pct_not_is_explicit_change"),
    )
)

result = (
    with_windows_2
    .filter(F.col("running_n_not_is_explicit") > 25)
    .select(
        "release_date_month_quintile",
        "min_greatest_duration_sec_bpm_max_6_prior_rank",
        "pct_not_is_explicit_change",
    )
    .distinct()
    .orderBy(F.col("pct_not_is_explicit_change").desc())
)
