from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified")
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_all_by_bpm_filled = Window.orderBy(F.col("bpm_filled").desc(), "id")
w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").desc_nulls_first())
w_all_by_bpm = Window.orderBy("bpm", "id")
w_all_by_bpm_filled_prior_3 = Window.orderBy(F.col("bpm_filled").desc(), "id").rowsBetween(-3, -1)
w_all_by_bpm_filled_prior_6 = Window.orderBy(F.col("bpm_filled").desc(), "id").rowsBetween(-6, -1)
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_artist_id_artist_debut_year = Window.partitionBy("artist_id", "artist_debut_year_dense_rank").orderBy("bpm_filled_max_6_prior_bucket", "id").rowsBetween(-1, -1)
w_artist_country_prev_release = Window.partitionBy("artist_country", F.date_trunc("month", "prev_release_date"))
w_artist_id_artist_debut_year_2 = Window.partitionBy("artist_id", "artist_debut_year_dense_rank").orderBy("bpm_filled_max_6_prior_bucket", "id")
w_artist_id_artist_debut_year_3 = Window.partitionBy("artist_id", "artist_debut_year_dense_rank").orderBy("bpm_filled_max_6_prior_bucket", "id").rowsBetween(-3, Window.currentRow)
w_artist_debut_year_dense_rank = Window.partitionBy("artist_debut_year_dense_rank").orderBy(F.col("release_date_year_min_bpm_filled").desc(), "id").rowsBetween(-1, -1)
w_artist_debut_year_dense_rank_2 = Window.partitionBy("artist_debut_year_dense_rank").orderBy(F.col("release_date_year_min_bpm_filled").desc(), "id")
w_last_release_date_year_n_row = Window.partitionBy(F.year("last_release_date"), "n_rows")
w_last_release_date_month_by_t = Window.partitionBy(F.date_trunc("month", "last_release_date")).orderBy(F.col("total_bpm_filled_change").asc_nulls_last(), "distinct_artist_id_in_release_date_year")

valid = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-08-26").cast("date"), F.lit("2025-08-02").cast("date"))) & (F.length(F.trim(F.col("title"))) >= 8))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled_2"),
    )
)

joined_artist = (
    valid.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("a.artist_name"))).like("L%")), "inner")
    .filter(F.col("a.is_verified"))
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

sequenced = (
    joined_artist
    .select(
        "*",
        F.size(F.collect_set("artist_country").over(w_artist_id_artist_is_verified)).alias("distinct_artist_country_in_artist_id_artist_is_v"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_release_date_year), F.lit(0)), 4).alias("duration_sec_share_of_release_date_year"),
        F.sum("bpm_filled").over(w_artist_id_artist_is_verified).alias("artist_id_artist_is_verified_sum_bpm_filled"),
        F.size(F.collect_set("artist_id").over(w_release_date_year)).alias("distinct_artist_id_in_release_date_year"),
        F.round(F.col("bpm_filled_2") / F.nullif(F.sum("bpm_filled_2").over(w_release_date_year), F.lit(0)), 4).alias("bpm_filled_2_share_of_release_date_year"),
        F.round(F.percent_rank().over(w_all_by_bpm_filled), 3).alias("bpm_filled_percent_rank"),
        F.dense_rank().over(w_artist_id_by_artist_debut_ye).alias("artist_debut_year_dense_rank"),
        F.lag("release_date").over(w_all_by_bpm).alias("prev_release_date"),
        F.round(F.avg("bpm").over(w_all_by_bpm_filled_prior_3), 2).alias("bpm_avg_3_prior"),
        F.min("bpm_filled").over(w_release_date_year).alias("release_date_year_min_bpm_filled"),
        F.max("bpm_filled").over(w_all_by_bpm_filled_prior_6).alias("bpm_filled_max_6_prior"),
    )
)

derived = (
    sequenced
    .filter(F.col("distinct_artist_country_in_artist_id_artist_is_v") >= 3)
    .select(
        "*",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        (F.floor(F.col("bpm_filled_max_6_prior") / 100) * 100).alias("bpm_filled_max_6_prior_bucket"),
    )
)

metrics = (
    derived
    .select(
        "*",
        F.size(F.collect_set("artist_country").over(w_release_date_month)).alias("distinct_artist_country_in_release_date_month"),
        F.sum("bpm_bucket").over(w_artist_id_artist_debut_year).alias("bpm_bucket_sum_1_prior"),
        F.round(F.col("bpm_avg_3_prior") / F.nullif(F.sum("bpm_avg_3_prior").over(w_artist_country_prev_release), F.lit(0)), 4).alias("bpm_avg_3_prior_share_of_artist_country_prev_rel"),
        F.row_number().over(w_artist_id_artist_debut_year_2).alias("seq_no"),
        F.max("bpm_filled").over(w_artist_id_artist_debut_year_3).alias("bpm_filled_max_4"),
        F.round(F.avg("bpm_avg_3_prior").over(w_artist_debut_year_dense_rank), 2).alias("bpm_avg_3_prior_avg_1_prior"),
        (F.col("bpm_filled") - F.lag("bpm_filled").over(w_artist_debut_year_dense_rank_2)).alias("bpm_filled_change"),
    )
)

grouped = (
    metrics
    .groupBy("distinct_artist_id_in_release_date_year")
    .agg(
        F.max("release_date").alias("last_release_date"),
        F.sum("bpm_filled_percent_rank").alias("total_bpm_filled_percent_rank"),
        F.round(100 * F.count(F.when(F.col("artist_country").isin("BR", "JP", "TH"), 1)) / F.count("*"), 1).alias("pct_artist_country_in_br_jp_th"),
        F.sum("bpm_filled_change").alias("total_bpm_filled_change"),
        F.min("bpm_avg_3_prior").alias("min_bpm_avg_3_prior"),
        F.count("*").alias("n_rows"),
        F.max("bpm_filled_percent_rank").alias("max_bpm_filled_percent_rank_check"),
    )
    .filter(F.col("max_bpm_filled_percent_rank_check") > 0.1)
    .drop("max_bpm_filled_percent_rank_check")
)

windowed = (
    grouped
    .select(
        "*",
        F.round(F.col("n_rows") / F.nullif(F.sum("n_rows").over(w_last_release_date_year_n_row), F.lit(0)), 4).alias("n_rows_share_of_last_release_date_year_n_rows"),
        F.rank().over(w_last_release_date_month_by_t).alias("total_bpm_filled_change_rank"),
        F.row_number().over(w_last_release_date_month_by_t).alias("rn"),
    )
)

result = (
    windowed
    .select(
        "distinct_artist_id_in_release_date_year",
        "total_bpm_filled_percent_rank",
        "total_bpm_filled_change",
        "min_bpm_avg_3_prior",
        "n_rows_share_of_last_release_date_year_n_rows",
        "total_bpm_filled_change_rank",
        F.round(F.col("min_bpm_avg_3_prior") * 1.5, 2).alias("min_bpm_avg_3_prior_adj"),
        F.abs(F.col("total_bpm_filled_change") - 450).alias("total_bpm_filled_change_distance"),
    )
)
