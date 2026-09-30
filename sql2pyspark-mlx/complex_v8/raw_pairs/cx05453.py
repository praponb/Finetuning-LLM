from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").desc(), "id")
w_all_by_release_date_running = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_artist_id_is_explicit_by_rel_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date", "id")
w_all_by_release_date_2 = Window.orderBy("release_date", "id")
w_release_date_quintile_last_k = Window.partitionBy("release_date_quintile", F.date_trunc("month", "last_known_release_date")).orderBy(F.col("release_date_seq").asc_nulls_last(), "id")
w_release_date_quintile_last_k_2 = Window.partitionBy("release_date_quintile", F.date_trunc("month", "last_known_release_date")).orderBy(F.col("release_date_seq").asc_nulls_last())
w_release_date_quintile_last_k_3 = Window.partitionBy("release_date_quintile", F.date_trunc("month", "last_known_release_date")).orderBy(F.col("release_date_seq").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_release_date_rank_2 = Window.partitionBy("release_date_rank_2")
w_release_date_seq_by_rn_ties = Window.partitionBy("release_date_seq").orderBy("rn")
w_release_date_quintile_last_k_4 = Window.partitionBy("release_date_quintile", F.date_trunc("month", "last_known_release_date")).orderBy(F.col("release_date_seq").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_seq_by_rn_movin = Window.partitionBy("release_date_seq").orderBy("rn", "id").rowsBetween(-1, Window.currentRow)

valid = (
    df_tracks
    .filter(~F.array_contains(F.col("moods"), "focus"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_release_date), 3).alias("release_date_percent_rank"),
        F.ntile(10).over(w_artist_id_is_explicit_by_rel).alias("release_date_decile"),
        F.last("release_date", ignorenulls=True).over(w_all_by_release_date_running).alias("last_known_release_date"),
        F.round(F.cume_dist().over(w_artist_id_by_release_date), 3).alias("release_date_cume_dist"),
        F.rank().over(w_artist_id_is_explicit_by_rel_2).alias("release_date_rank"),
        F.ntile(5).over(w_artist_id_is_explicit_by_rel_2).alias("release_date_quintile"),
        F.row_number().over(w_artist_id_by_release_date).alias("seq_no"),
        F.first("release_date", ignorenulls=True).over(w_all_by_release_date_running).alias("first_known_release_date"),
        F.row_number().over(w_all_by_release_date_2).alias("release_date_seq"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_is_explicit_by_rel_2)).alias("days_since_prev_release_date"),
        F.rank().over(w_all_by_release_date_2).alias("release_date_rank_2"),
        F.lead("release_date", 3).over(w_artist_id_by_release_date).alias("next3_release_date"),
        F.row_number().over(w_all_by_release_date).alias("rn"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "release_date_percent_rank",
        "release_date_decile",
        "last_known_release_date",
        "release_date_cume_dist",
        "release_date_quintile",
        "seq_no",
        "release_date_rank_2",
        "next3_release_date",
        (F.col("release_date_cume_dist") - F.lag("release_date_cume_dist").over(w_release_date_quintile_last_k)).alias("release_date_cume_dist_change"),
        F.round(F.cume_dist().over(w_release_date_quintile_last_k_2), 3).alias("release_date_seq_cume_dist"),
        F.round(F.avg("seq_no").over(w_release_date_quintile_last_k_3), 2).alias("seq_no_avg_2_prior"),
        F.round(F.percent_rank().over(w_release_date_quintile_last_k_2), 3).alias("release_date_seq_percent_rank"),
        F.max("release_date_decile").over(w_release_date_rank_2).alias("release_date_rank_2_max_release_date_decile"),
        F.round(F.percent_rank().over(w_release_date_seq_by_rn_ties), 3).alias("rn_percent_rank"),
        F.nth_value("release_date_seq", 2).over(w_release_date_quintile_last_k_4).alias("second_release_date_seq"),
        F.max("release_date_rank").over(w_release_date_seq_by_rn_movin).alias("release_date_rank_max_2"),
    )
)

grouped = (
    ranked
    .groupBy(F.date_format("release_date", "yyyy-MM").alias("release_date_ym"))
    .agg(
        F.percentile_approx("release_date_rank_2", 0.5).alias("median_release_date_rank_2"),
        F.max("release_date_rank_2").alias("max_release_date_rank_2"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_is_explicit_false"),
        F.round(F.avg("release_date_percent_rank"), 2).alias("avg_release_date_percent_rank"),
    )
)

result = (
    grouped
    .select(
        "release_date_ym",
        "median_release_date_rank_2",
        "max_release_date_rank_2",
        "avg_release_date_percent_rank",
    )
    .orderBy(F.col("median_release_date_rank_2").desc_nulls_first())
)
