from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_pr = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(-2, -1)
w_all_by_bpm = Window.orderBy("bpm", "id")
w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy("duration_sec")
w_all_by_bpm_moving_2 = Window.orderBy("bpm", "id").rowsBetween(-2, Window.currentRow)
w_release_date_plus_3m_month_b = Window.partitionBy(F.month("release_date_plus_3m")).orderBy("duration_sec_per_bpm", "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy("duration_sec", "id")
w_all_by_bpm_ties = Window.orderBy("bpm")
w_all_by_bpm_running = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_duration = Window.partitionBy(F.date_trunc("month", "release_date"), "duration_sec_per_bpm_seq").orderBy("bpm_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm_percent_rank_full = Window.orderBy("bpm_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_bpm_rank_bpm_decile = Window.partitionBy("bpm_rank", "bpm_decile")
w_bpm_decile_artist_id_by_firs = Window.partitionBy("bpm_decile", "artist_id").orderBy("first_known_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm_percent_rank_ties = Window.orderBy("bpm_percent_rank")

recent = (
    df_tracks
    .filter((F.col("artist_id") <= 5) & (F.array_contains(F.col("moods"), "focus")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.add_months(F.col("release_date"), 3).alias("release_date_plus_3m"),
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.round(F.avg("duration_sec_per_bpm").over(w_artist_id_by_duration_sec_pr), 2).alias("duration_sec_per_bpm_avg_2_prior"),
        F.ntile(5).over(w_all_by_bpm).alias("bpm_quintile"),
        F.dense_rank().over(w_artist_id_by_duration_sec_ti).alias("duration_sec_dense_rank"),
        F.max("duration_sec_per_bpm").over(w_all_by_bpm_moving_2).alias("duration_sec_per_bpm_max_3"),
        F.row_number().over(w_release_date_plus_3m_month_b).alias("duration_sec_per_bpm_seq"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.ntile(10).over(w_all_by_bpm).alias("bpm_decile"),
        F.row_number().over(w_all_by_bpm).alias("rn"),
        F.round(F.cume_dist().over(w_all_by_bpm), 3).alias("bpm_cume_dist"),
        F.lead("duration_sec_per_bpm").over(w_artist_id_by_duration_sec).alias("next_duration_sec_per_bpm"),
        F.rank().over(w_all_by_bpm_ties).alias("bpm_rank"),
        F.round(F.percent_rank().over(w_artist_id_by_duration_sec_ti), 3).alias("duration_sec_percent_rank"),
        F.first("bpm", ignorenulls=True).over(w_all_by_bpm_running).alias("first_known_bpm"),
        F.round(F.percent_rank().over(w_all_by_bpm), 3).alias("bpm_percent_rank"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "duration_sec_per_bpm_avg_2_prior",
        "bpm_quintile",
        "duration_sec_per_bpm_max_3",
        "duration_sec_per_bpm_seq",
        "rows_in_is_explicit_artist_id",
        "bpm_decile",
        "bpm_cume_dist",
        "next_duration_sec_per_bpm",
        "bpm_rank",
        "duration_sec_percent_rank",
        "first_known_bpm",
        "bpm_percent_rank",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.first("first_known_bpm", ignorenulls=True).over(w_release_date_month_duration).alias("first_known_first_known_bpm"),
        F.nth_value("duration_sec_percent_rank", 2).over(w_all_by_bpm_percent_rank_full).alias("second_duration_sec_percent_rank"),
        F.round(F.col("first_known_bpm") / F.nullif(F.sum("first_known_bpm").over(w_bpm_rank_bpm_decile), F.lit(0)), 4).alias("first_known_bpm_share_of_bpm_rank_bpm_decile"),
        F.sum("bpm_quintile").over(w_bpm_decile_artist_id_by_firs).alias("running_bpm_quintile"),
        F.dense_rank().over(w_all_by_bpm_percent_rank_ties).alias("bpm_percent_rank_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "duration_sec_per_bpm_avg_2_prior",
        "bpm_quintile",
        "duration_sec_per_bpm_max_3",
        "duration_sec_per_bpm_seq",
        "rows_in_is_explicit_artist_id",
        "bpm_cume_dist",
        "next_duration_sec_per_bpm",
        "bpm_rank",
        "duration_sec_percent_rank",
        "first_known_bpm",
        "bpm_percent_rank",
        "title_clean",
        "second_duration_sec_percent_rank",
        "running_bpm_quintile",
        "bpm_percent_rank_dense_rank",
        F.concat_ws("-", F.col("title_clean"), F.col("title")).alias("title_clean_title_label"),
        F.initcap(F.trim(F.col("title_clean"))).alias("title_clean_clean"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("title_clean_title_label").desc())
)
