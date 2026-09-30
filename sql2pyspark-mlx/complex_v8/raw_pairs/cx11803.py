from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_duration_sec_tie = Window.partitionBy("is_explicit", "duration_sec_tier")
w_release_date_year_year = Window.partitionBy(F.year("release_date_year"))
w_all_by_release_date_year_run = Window.orderBy(F.col("release_date_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_duration_sec_tier = Window.partitionBy("artist_id", "duration_sec_tier").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_release_date_year_tie = Window.orderBy(F.col("release_date_year").desc())
w_artist_id_by_bpm_ties = Window.partitionBy("artist_id").orderBy("bpm")
w_all_by_duration_sec_prior_1 = Window.orderBy("duration_sec", "id").rowsBetween(-1, -1)
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_sec_tier_by_duratio = Window.partitionBy("duration_sec_tier").orderBy("duration_sec_vs_is_explicit_duration_sec_tier_av", "id").rowsBetween(-1, Window.currentRow)
w_all_by_release_date_year_cum = Window.orderBy("release_date_year_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_duration_sec_tier_by_duratio_2 = Window.partitionBy("duration_sec_tier").orderBy("duration_sec_vs_is_explicit_duration_sec_tier_av", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_duration_sec_tier_by_duratio_3 = Window.partitionBy("duration_sec_tier").orderBy("duration_sec_vs_is_explicit_duration_sec_tier_av", "id")
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.date_trunc("month", "release_date_year")).orderBy("release_date_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_year_cum_2 = Window.orderBy("release_date_year_cume_dist", "id").rowsBetween(-3, Window.currentRow)
w_duration_sec_tier_artist_id = Window.partitionBy("duration_sec_tier", "artist_id")
w_duration_sec_tier = Window.partitionBy("duration_sec_tier")

recent = (
    df_tracks
    .filter(F.col("bpm") < 155)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("duration_sec") >= 402, "large").when(F.col("duration_sec") >= 164, "medium").otherwise("small").alias("duration_sec_tier"),
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

with_windows = (
    recent
    .select(
        "*",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_duration_sec_tie), F.lit(0)), 4).alias("bpm_share_of_is_explicit_duration_sec_tier"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_release_date_year_year), F.lit(0)), 4).alias("bpm_share_of_release_date_year_year"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit_duration_sec_tie), 2).alias("duration_sec_vs_is_explicit_duration_sec_tier_av"),
        F.count(F.lit(1)).over(w_release_date_year_year).alias("rows_in_release_date_year_year"),
        F.last("bpm", ignorenulls=True).over(w_all_by_release_date_year_run).alias("last_known_bpm"),
        F.max("bpm").over(w_artist_id_duration_sec_tier).alias("max_bpm_so_far"),
        F.max("bpm").over(w_is_explicit_duration_sec_tie).alias("is_explicit_duration_sec_tier_max_bpm"),
        F.round(F.cume_dist().over(w_all_by_release_date_year_tie), 3).alias("release_date_year_cume_dist"),
        F.sum("duration_sec").over(w_all_by_release_date_year_run).alias("running_duration_sec"),
        F.round(F.cume_dist().over(w_artist_id_by_bpm_ties), 3).alias("bpm_cume_dist"),
        F.count(F.lit(1)).over(w_is_explicit_duration_sec_tie).alias("rows_in_is_explicit_duration_sec_tier"),
        F.max("bpm").over(w_all_by_duration_sec_prior_1).alias("bpm_max_1_prior"),
        F.dense_rank().over(w_all_by_release_date_year_tie).alias("release_date_year_dense_rank"),
        F.last("bpm", ignorenulls=True).over(w_all_by_duration_sec_running).alias("last_known_bpm_2"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("release_date_year_dense_rank") == 1) & (F.col("duration_sec_vs_is_explicit_duration_sec_tier_av") < 294.0))
    .drop("release_date_year_dense_rank")
)

windowed = (
    top_per_group
    .select(
        "*",
        F.round(F.avg("last_known_bpm").over(w_duration_sec_tier_by_duratio), 2).alias("last_known_bpm_avg_2"),
        F.nth_value("last_known_bpm_2", 2).over(w_all_by_release_date_year_cum).alias("second_last_known_bpm_2"),
        F.nth_value("last_known_bpm_2", 2).over(w_duration_sec_tier_by_duratio_2).alias("second_last_known_bpm_2_2"),
        F.ntile(5).over(w_duration_sec_tier_by_duratio_3).alias("duration_sec_vs_is_explicit_duration_sec_tier_av_2"),
        F.nth_value("last_known_bpm_2", 2).over(w_artist_id_release_date_year).alias("second_last_known_bpm_2_3"),
        F.sum("max_bpm_so_far").over(w_all_by_release_date_year_cum_2).alias("max_bpm_so_far_sum_4"),
        F.round(F.col("release_date_year_cume_dist") - F.avg("release_date_year_cume_dist").over(w_duration_sec_tier_artist_id), 2).alias("release_date_year_cume_dist_vs_duration_sec_tier"),
        F.size(F.collect_set("artist_id").over(w_duration_sec_tier)).alias("distinct_artist_id_in_duration_sec_tier"),
    )
)

result = (
    windowed
    .filter(F.col("max_bpm_so_far") <= 1191)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "release_date_year",
        "bpm_share_of_is_explicit_duration_sec_tier",
        "bpm_share_of_release_date_year_year",
        "duration_sec_vs_is_explicit_duration_sec_tier_av",
        "rows_in_release_date_year_year",
        "last_known_bpm",
        "max_bpm_so_far",
        "release_date_year_cume_dist",
        "running_duration_sec",
        "bpm_cume_dist",
        "rows_in_is_explicit_duration_sec_tier",
        "bpm_max_1_prior",
        "last_known_bpm_2",
        "last_known_bpm_avg_2",
        "second_last_known_bpm_2",
        "second_last_known_bpm_2_2",
        "second_last_known_bpm_2_3",
        "release_date_year_cume_dist_vs_duration_sec_tier",
        "distinct_artist_id_in_duration_sec_tier",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.greatest(F.col("last_known_bpm_2"), F.col("max_bpm_so_far")).alias("greatest_last_known_bpm_2_max_bpm_so_far"),
    )
    .orderBy("last_known_bpm_avg_2", "max_bpm_so_far")
)
