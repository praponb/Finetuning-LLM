from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_title_len_artist_id = Window.partitionBy("title_len", "artist_id")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_release_date_week_to_date_ar = Window.partitionBy(F.to_date("release_date_week"), "artist_id").orderBy(F.col("release_date_week").desc(), "id")
w_title_len_by_release_date_we = Window.partitionBy("title_len").orderBy("release_date_week")
w_release_date_month = Window.partitionBy(F.month("release_date"))
w_release_date_year_by_duratio = Window.partitionBy(F.year("release_date")).orderBy(F.col("duration_sec").desc())
w_release_date_year_by_duratio_2 = Window.partitionBy(F.year("release_date")).orderBy(F.col("duration_sec").desc(), "id")
w_release_date_week_to_date_ar_2 = Window.partitionBy(F.to_date("release_date_week"), "artist_id").orderBy(F.col("release_date_week").desc(), "id").rowsBetween(-1, -1)
w_release_date_month_artist_id = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy(F.col("release_date_week").desc())
w_release_date_month_artist_id_2 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy(F.col("release_date_week").desc(), "id")
w_duration_sec_dense_rank_rn = Window.partitionBy("duration_sec_dense_rank", "rn")
w_title_len_by_release_date_we_2 = Window.partitionBy("title_len").orderBy(F.col("release_date_week").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_title_len_by_bpm_vs_title = Window.partitionBy("rn", "title_len").orderBy(F.col("bpm_vs_title_len_artist_id_avg").desc())
w_release_date_week_month_by_p = Window.partitionBy(F.month("release_date_week")).orderBy("prev3_release_date", "id")
w_title_len_by_release_date_we_3 = Window.partitionBy("title_len").orderBy(F.col("release_date_week").asc_nulls_last(), "id")

filtered = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "release_date",
        "moods",
        "release_date_week",
        "title_len",
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_title_len_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_title_len_artist_id"),
        F.lag("duration_sec").over(w_all_by_duration_sec).alias("prev_duration_sec"),
        F.row_number().over(w_release_date_week_to_date_ar).alias("rn"),
        F.dense_rank().over(w_title_len_by_release_date_we).alias("release_date_week_dense_rank"),
        F.sum("bpm").over(w_release_date_month).alias("release_date_month_sum_bpm"),
        F.dense_rank().over(w_release_date_year_by_duratio).alias("duration_sec_dense_rank"),
        F.lag("release_date_week").over(w_release_date_year_by_duratio_2).alias("prev_release_date_week"),
        F.max("duration_sec").over(w_release_date_week_to_date_ar_2).alias("duration_sec_max_1_prior"),
        F.count(F.lit(1)).over(w_release_date_month).alias("rows_in_release_date_month"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_title_len_artist_id), 2).alias("bpm_vs_title_len_artist_id_avg"),
        F.rank().over(w_release_date_month_artist_id).alias("release_date_week_rank"),
        F.round(F.avg("title_len").over(w_title_len_artist_id), 2).alias("title_len_artist_id_avg_title_len"),
        F.min("bpm").over(w_title_len_artist_id).alias("title_len_artist_id_min_bpm"),
        F.lag("release_date", 3).over(w_release_date_month_artist_id_2).alias("prev3_release_date"),
    )
)

top_rows = sequenced.filter(F.col("release_date_week_rank") <= 5)

with_trends = (
    top_rows
    .select(
        "id",
        "moods",
        "title_len",
        "prev_duration_sec",
        "rn",
        "prev_release_date_week",
        "duration_sec_max_1_prior",
        "rows_in_release_date_month",
        "bpm_vs_title_len_artist_id_avg",
        "release_date_week_rank",
        "title_len_artist_id_avg_title_len",
        "title_len_artist_id_min_bpm",
        F.max("rn").over(w_duration_sec_dense_rank_rn).alias("duration_sec_dense_rank_rn_max_rn"),
        F.first("duration_sec_max_1_prior", ignorenulls=True).over(w_title_len_by_release_date_we_2).alias("first_known_duration_sec_max_1_prior"),
        F.round(F.percent_rank().over(w_rn_title_len_by_bpm_vs_title), 3).alias("bpm_vs_title_len_artist_id_avg_percent_rank"),
        F.lag("release_date_week_dense_rank").over(w_release_date_week_month_by_p).alias("prev_release_date_week_dense_rank"),
        F.count(F.lit(1)).over(w_duration_sec_dense_rank_rn).alias("rows_in_duration_sec_dense_rank_rn"),
        F.rank().over(w_rn_title_len_by_bpm_vs_title).alias("bpm_vs_title_len_artist_id_avg_rank"),
        F.round(F.cume_dist().over(w_title_len_by_release_date_we_3), 3).alias("release_date_week_cume_dist"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "moods",
        "rn",
        "prev_release_date_week",
        "duration_sec_max_1_prior",
        "bpm_vs_title_len_artist_id_avg",
        "release_date_week_rank",
        "title_len_artist_id_min_bpm",
        "duration_sec_dense_rank_rn_max_rn",
        "first_known_duration_sec_max_1_prior",
        "prev_release_date_week_dense_rank",
        "rows_in_duration_sec_dense_rank_rn",
        "bpm_vs_title_len_artist_id_avg_rank",
        "release_date_week_cume_dist",
        (F.col("duration_sec_max_1_prior") + F.col("release_date_week_rank")).alias("duration_sec_max_1_prior_plus_release_date_week_"),
        F.hour(F.col("prev_release_date_week")).alias("prev_release_date_week_hour"),
    )
)
