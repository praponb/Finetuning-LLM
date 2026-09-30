from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year_to_date = Window.partitionBy(F.to_date("release_date_year"))
w_artist_id_title_len_by_bpm = Window.partitionBy("artist_id", "title_len").orderBy("bpm", "id")
w_all_by_title_len_moving_2 = Window.orderBy("title_len", "id").rowsBetween(-2, Window.currentRow)
w_all_by_title_len = Window.orderBy("title_len", "id")
w_title_len = Window.partitionBy("title_len")
w_all_by_title_len_ties = Window.orderBy("title_len")
w_all_by_title_len_prior_3 = Window.orderBy("title_len", "id").rowsBetween(-3, -1)
w_artist_id_title_len_by_bpm_t = Window.partitionBy("artist_id", "title_len").orderBy("bpm")
w_release_date_year_to_date_ti = Window.partitionBy(F.to_date("release_date_year"), "title_len").orderBy(F.col("release_date_year").asc_nulls_last())
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_all_by_duration_sec_moving_2 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-2, Window.currentRow)
w_artist_id_title_len = Window.partitionBy("artist_id", "title_len")
w_all_by_bpm_share_of_artist_i = Window.orderBy("bpm_share_of_artist_id_title_len", "id")
w_artist_id_by_distinct_artist = Window.partitionBy("artist_id").orderBy(F.col("distinct_artist_id_in_title_len").desc(), "id")
w_artist_id_release_date_year = Window.partitionBy("artist_id", "release_date_year_rank").orderBy(F.col("title_len_max_3_prior").desc_nulls_first(), "id")
w_all_by_bpm_share_of_artist_i_2 = Window.orderBy("bpm_share_of_artist_id_title_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tracks = (
    df_tracks
    .filter(F.col("duration_sec") >= 183)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

with_windows = (
    scoped_tracks
    .select(
        "*",
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_release_date_year_to_date), F.lit(0)), 4).alias("duration_sec_share_of_release_date_year_to_date"),
        F.lag("bpm", 3).over(w_artist_id_title_len_by_bpm).alias("prev3_bpm"),
        F.round(F.avg("duration_sec").over(w_all_by_title_len_moving_2), 2).alias("duration_sec_avg_3"),
        F.row_number().over(w_all_by_title_len).alias("rn"),
        F.size(F.collect_set("artist_id").over(w_title_len)).alias("distinct_artist_id_in_title_len"),
        F.row_number().over(w_artist_id_title_len_by_bpm).alias("bpm_seq"),
        F.round(F.cume_dist().over(w_all_by_title_len_ties), 3).alias("title_len_cume_dist"),
        F.size(F.collect_set("artist_id").over(w_release_date_year_to_date)).alias("distinct_artist_id_in_release_date_year_to_date"),
        F.round(F.col("title_len") - F.avg("title_len").over(w_title_len), 2).alias("title_len_vs_title_len_avg"),
        F.max("title_len").over(w_all_by_title_len_prior_3).alias("title_len_max_3_prior"),
        F.round(F.percent_rank().over(w_artist_id_title_len_by_bpm_t), 3).alias("bpm_percent_rank"),
        F.rank().over(w_release_date_year_to_date_ti).alias("release_date_year_rank"),
        F.dense_rank().over(w_all_by_duration_sec_ties).alias("duration_sec_dense_rank"),
        F.round(F.avg("duration_sec").over(w_all_by_duration_sec_moving_2), 2).alias("duration_sec_avg_3_2"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_title_len), F.lit(0)), 4).alias("bpm_share_of_artist_id_title_len"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        (F.col("title_len_max_3_prior") - F.lag("title_len_max_3_prior").over(w_all_by_bpm_share_of_artist_i)).alias("title_len_max_3_prior_change"),
        (F.col("release_date_year_rank") - F.lag("release_date_year_rank").over(w_artist_id_by_distinct_artist)).alias("release_date_year_rank_change"),
        F.round(F.percent_rank().over(w_artist_id_by_distinct_artist), 3).alias("distinct_artist_id_in_title_len_percent_rank"),
        F.row_number().over(w_artist_id_release_date_year).alias("title_len_max_3_prior_seq"),
        F.round(F.avg("title_len_cume_dist").over(w_title_len), 2).alias("title_len_avg_title_len_cume_dist"),
        F.count(F.lit(1)).over(w_all_by_bpm_share_of_artist_i_2).alias("running_count"),
        F.round(F.col("title_len_vs_title_len_avg") / F.nullif(F.sum("title_len_vs_title_len_avg").over(w_title_len), F.lit(0)), 4).alias("title_len_vs_title_len_avg_share_of_title_len"),
        F.round(F.col("title_len_vs_title_len_avg") - F.avg("title_len_vs_title_len_avg").over(w_title_len), 2).alias("title_len_vs_title_len_avg_vs_title_len_avg"),
    )
)

result = (
    metrics
    .select(
        "title_len_avg_title_len_cume_dist",
        "title_len_vs_title_len_avg_share_of_title_len",
    )
    .distinct()
    .orderBy(
        F.col("title_len_vs_title_len_avg_share_of_title_len").desc(),
        F.col("title_len_avg_title_len_cume_dist").desc(),
    )
)
