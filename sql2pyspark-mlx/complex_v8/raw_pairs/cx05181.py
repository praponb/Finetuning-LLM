from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm_ties = Window.orderBy("bpm")
w_release_date_month_by_releas = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("release_date").desc(), "id")
w_all_by_bpm_running = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_by_bpm_ties = Window.partitionBy("is_explicit").orderBy("bpm")
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_artist_id_by_release_date_ti = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc())
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy("bpm", "id")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_seq_no_prev_release_date_yea = Window.partitionBy("seq_no", F.year("prev_release_date")).orderBy("days_since_prev_release_date_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev_release_date_month = Window.partitionBy(F.date_trunc("month", "prev_release_date"))
w_release_date_rank_by_bpm_ran = Window.partitionBy("release_date_rank").orderBy("bpm_rank", "id").rowsBetween(-6, Window.currentRow)
w_release_date_rank_by_bpm_ran_2 = Window.partitionBy("release_date_rank").orderBy("bpm_rank", "id").rowsBetween(-6, -1)
w_seq_no_prev_release_date_yea_2 = Window.partitionBy("seq_no", F.year("prev_release_date")).orderBy("days_since_prev_release_date_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_seq_no_prev_release_date_yea_3 = Window.partitionBy("seq_no", F.year("prev_release_date")).orderBy("days_since_prev_release_date_2", "id")
w_release_date_rank_by_bpm_ran_3 = Window.partitionBy("release_date_rank").orderBy("bpm_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("C%")) | (F.col("artist_id") <= 6))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.rank().over(w_all_by_bpm_ties).alias("bpm_rank"),
        F.lag("release_date").over(w_release_date_month_by_releas).alias("prev_release_date"),
        F.last("release_date", ignorenulls=True).over(w_all_by_bpm_running).alias("last_known_release_date"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit), 2).alias("bpm_vs_is_explicit_avg"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_release_date_month_by_releas)).alias("days_since_prev_release_date"),
        F.round(F.percent_rank().over(w_is_explicit_by_bpm_ties), 3).alias("bpm_percent_rank"),
        F.ntile(10).over(w_release_date_month_by_releas).alias("release_date_decile"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.nth_value("bpm", 2).over(w_all_by_bpm_full).alias("second_bpm"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_by_release_date)).alias("days_since_prev_release_date_2"),
        F.rank().over(w_artist_id_by_release_date_ti).alias("release_date_rank"),
        F.row_number().over(w_is_explicit_by_bpm).alias("seq_no"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_bpm)).alias("days_since_prev_release_date_3"),
    )
)

first_rows = sequenced.filter(F.col("seq_no") <= 2)

metrics = (
    first_rows
    .select(
        "*",
        F.last("days_since_prev_release_date").over(w_seq_no_prev_release_date_yea).alias("last_days_since_prev_release_date"),
        F.min("release_date_rank").over(w_prev_release_date_month).alias("prev_release_date_month_min_release_date_rank"),
        F.sum("days_since_prev_release_date").over(w_release_date_rank_by_bpm_ran).alias("days_since_prev_release_date_sum_7"),
        F.round(F.avg("bpm").over(w_release_date_rank_by_bpm_ran_2), 2).alias("bpm_avg_6_prior"),
        F.min("bpm").over(w_seq_no_prev_release_date_yea_2).alias("min_bpm_so_far"),
        F.round(F.cume_dist().over(w_seq_no_prev_release_date_yea_3), 3).alias("days_since_prev_release_date_2_cume_dist"),
        F.first("prev_release_date").over(w_release_date_rank_by_bpm_ran_3).alias("first_prev_release_date"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        "title_clean",
        "bpm_rank",
        "prev_release_date",
        "last_known_release_date",
        "bpm_vs_is_explicit_avg",
        "bpm_percent_rank",
        "release_date_decile",
        "rows_in_is_explicit",
        "second_bpm",
        "release_date_rank",
        "seq_no",
        "last_days_since_prev_release_date",
        "prev_release_date_month_min_release_date_rank",
        "days_since_prev_release_date_sum_7",
        "days_since_prev_release_date_2_cume_dist",
        "first_prev_release_date",
    )
    .orderBy(
        F.col("days_since_prev_release_date_sum_7").asc_nulls_last(),
        F.col("bpm_rank").desc(),
        "id",
    )
    .limit(10)
)
