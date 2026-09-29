from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_filled = Window.orderBy(F.col("duration_sec_filled").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_duration_sec_fi = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_filled").desc(), "id")
w_artist_id_by_duration_sec_fi_2 = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_filled").asc_nulls_last(), "id")
w_all_by_duration_sec_prior_6 = Window.orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_artist_id_by_duration_sec_fi_3 = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")

base = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

ranked = (
    base
    .select(
        "id",
        "title",
        "duration_sec",
        "duration_sec_filled",
        F.ntile(5).over(w_all_by_duration_sec_filled).alias("duration_sec_filled_quintile"),
        F.round(F.avg("duration_sec_filled").over(w_artist_id), 2).alias("artist_id_avg_duration_sec_filled"),
        F.row_number().over(w_all_by_duration_sec_filled).alias("rn"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.rank().over(w_artist_id_by_duration_sec_fi).alias("duration_sec_filled_rank"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_duration_sec_fi_2)).alias("duration_sec_change"),
        (F.col("duration_sec_filled") - F.lag("duration_sec_filled").over(w_artist_id_by_duration_sec_fi)).alias("duration_sec_filled_change"),
        F.round(F.col("duration_sec_filled") - F.avg("duration_sec_filled").over(w_artist_id), 2).alias("duration_sec_filled_vs_artist_id_avg"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.sum("duration_sec_filled").over(w_all_by_duration_sec_prior_6).alias("duration_sec_filled_sum_6_prior"),
        F.dense_rank().over(w_artist_id_by_duration_sec_fi).alias("duration_sec_filled_dense_rank"),
        F.sum("duration_sec_filled").over(w_artist_id_by_duration_sec_fi_3).alias("running_duration_sec_filled"),
        F.max("duration_sec_filled").over(w_artist_id).alias("artist_id_max_duration_sec_filled"),
        F.min("duration_sec_filled").over(w_artist_id).alias("artist_id_min_duration_sec_filled"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
    )
)

leaders = ranked.filter((F.col("rn") <= 3) & (F.col("artist_id_avg_duration_sec_filled") < 321.0))

result = (
    leaders
    .filter(F.col("artist_id_min_duration_sec_filled") < 3172)
    .select(
        "id",
        "title",
        "duration_sec",
        "duration_sec_filled",
        "duration_sec_filled_quintile",
        "artist_id_avg_duration_sec_filled",
        "rn",
        "duration_sec_filled_rank",
        "duration_sec_change",
        "duration_sec_filled_change",
        "duration_sec_filled_vs_artist_id_avg",
        "duration_sec_share_of_artist_id",
        "duration_sec_filled_sum_6_prior",
        "duration_sec_filled_dense_rank",
        "running_duration_sec_filled",
        "artist_id_max_duration_sec_filled",
        "artist_id_min_duration_sec_filled",
        "duration_sec_percent_rank",
        F.round(F.col("duration_sec") / F.nullif(F.col("duration_sec_filled_rank"), F.lit(0)), 3).alias("duration_sec_per_duration_sec_filled_rank"),
        F.least(F.col("duration_sec"), F.col("rn")).alias("least_duration_sec_rn"),
    )
    .orderBy(F.col("artist_id_max_duration_sec_filled").asc_nulls_last())
)
