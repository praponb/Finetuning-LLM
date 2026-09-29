from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm_distance = Window.partitionBy("is_explicit").orderBy(F.col("bpm_distance_is_explicit_true").desc(), "is_explicit")
w_is_explicit_by_bpm_distance_2 = Window.partitionBy("is_explicit").orderBy(F.col("bpm_distance_is_explicit_true").desc())
w_is_explicit_bpm_distance_is = Window.partitionBy("is_explicit", "bpm_distance_is_explicit_true_dense_rank").orderBy(F.col("bpm_distance_is_explicit_true_rank").desc(), "is_explicit")
w_bpm_distance_is_explicit_tru = Window.partitionBy("bpm_distance_is_explicit_true_dense_rank", "bpm_distance_is_explicit_true_rank").orderBy("bpm_distance_is_explicit_true_cume_dist", "is_explicit")
w_is_explicit_bpm_distance_is_2 = Window.partitionBy("is_explicit", "bpm_distance_is_explicit_true_dense_rank").orderBy(F.col("bpm_distance_is_explicit_true_rank").desc())
w_all_by_rn = Window.orderBy(F.col("rn").asc_nulls_last(), "is_explicit")
w_bpm_distance_is_explicit_tru_2 = Window.partitionBy("bpm_distance_is_explicit_true_rank_rank")

valid = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("bpm") - 143).alias("bpm_distance"),
    )
)

stats = (
    valid
    .groupBy("is_explicit")
    .agg(
        F.sum(F.when(F.col("is_explicit"), F.col("bpm_distance")).otherwise(0)).alias("bpm_distance_is_explicit_true"),
        F.sum(F.when(F.col("is_explicit"), F.col("bpm_distance"))).alias("bpm_distance_is_explicit_true_2"),
    )
)

with_windows = (
    stats
    .select(
        "*",
        F.rank().over(w_is_explicit_by_bpm_distance).alias("bpm_distance_is_explicit_true_rank"),
        F.rank().over(w_is_explicit_by_bpm_distance_2).alias("bpm_distance_is_explicit_true_rank_2"),
        F.round(F.cume_dist().over(w_is_explicit_by_bpm_distance_2), 3).alias("bpm_distance_is_explicit_true_cume_dist"),
        F.dense_rank().over(w_is_explicit_by_bpm_distance_2).alias("bpm_distance_is_explicit_true_dense_rank"),
    )
)

qualified = (
    with_windows
    .crossJoin(with_windows.agg(F.min("bpm_distance_is_explicit_true_rank_2").alias("overall_min_bpm_distance_is_explicit_true_rank_2")))
    .filter(F.col("bpm_distance_is_explicit_true_rank_2") > F.col("overall_min_bpm_distance_is_explicit_true_rank_2"))
    .drop("overall_min_bpm_distance_is_explicit_true_rank_2")
)

ranked = (
    qualified
    .select(
        "is_explicit",
        "bpm_distance_is_explicit_true_rank",
        "bpm_distance_is_explicit_true_dense_rank",
        F.round(F.percent_rank().over(w_is_explicit_bpm_distance_is), 3).alias("bpm_distance_is_explicit_true_rank_percent_rank"),
        F.round(F.cume_dist().over(w_bpm_distance_is_explicit_tru), 3).alias("bpm_distance_is_explicit_true_cume_dist_cume_dis"),
        F.ntile(3).over(w_bpm_distance_is_explicit_tru).alias("bpm_distance_is_explicit_true_cume_dist_tercile"),
        F.rank().over(w_bpm_distance_is_explicit_tru).alias("bpm_distance_is_explicit_true_cume_dist_rank"),
        F.row_number().over(w_bpm_distance_is_explicit_tru).alias("rn"),
        F.rank().over(w_is_explicit_bpm_distance_is_2).alias("bpm_distance_is_explicit_true_rank_rank"),
    )
)

with_windows_2 = (
    ranked
    .select(
        "is_explicit",
        "bpm_distance_is_explicit_true_rank",
        "bpm_distance_is_explicit_true_dense_rank",
        "bpm_distance_is_explicit_true_rank_percent_rank",
        "bpm_distance_is_explicit_true_cume_dist_cume_dis",
        "bpm_distance_is_explicit_true_cume_dist_tercile",
        "bpm_distance_is_explicit_true_cume_dist_rank",
        "rn",
        F.row_number().over(w_all_by_rn).alias("rn_seq"),
        F.round(F.avg("rn").over(w_bpm_distance_is_explicit_tru_2), 2).alias("bpm_distance_is_explicit_true_rank_rank_avg_rn"),
    )
)

result = (
    with_windows_2
    .select(
        "is_explicit",
        "bpm_distance_is_explicit_true_dense_rank",
        "bpm_distance_is_explicit_true_rank_percent_rank",
        "bpm_distance_is_explicit_true_cume_dist_cume_dis",
        "bpm_distance_is_explicit_true_cume_dist_tercile",
        "bpm_distance_is_explicit_true_cume_dist_rank",
        "rn",
        "rn_seq",
        "bpm_distance_is_explicit_true_rank_rank_avg_rn",
        (F.floor(F.col("rn_seq") / 2) * 2).alias("rn_seq_bucket"),
    )
    .orderBy(F.col("rn_seq").desc(), "is_explicit")
    .limit(10)
)
