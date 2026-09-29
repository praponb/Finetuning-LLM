from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.month("release_date"))
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-6, -1)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy("duration_sec", "id")
w_release_date_month_artist_id_2 = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_release_date_month_artist_id_3 = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("duration_sec").desc())
w_release_date_month_artist_id_4 = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_all_by_duration_sec_cume_dis = Window.orderBy("duration_sec_cume_dist")
w_all_by_duration_sec_cume_dis_2 = Window.orderBy("duration_sec_cume_dist", "id")
w_duration_sec_rank_artist_id = Window.partitionBy("duration_sec_rank", "artist_id")
w_artist_id_duration_sec_rank = Window.partitionBy("artist_id", "duration_sec_rank").orderBy(F.col("duration_sec_cume_dist_2").desc(), "id")

valid_tracks = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-04-29").cast("date"), F.lit("2025-03-29").cast("date"))) & (F.col("duration_sec") >= 209))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

metrics = (
    valid_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_release_date_mon), F.lit(0)), 4).alias("bpm_share_of_is_explicit_release_date_month"),
        F.lag("bpm", 3).over(w_all_by_duration_sec).alias("prev3_bpm"),
        F.max("bpm").over(w_release_date_month_artist_id).alias("bpm_max_6_prior"),
        F.round(F.percent_rank().over(w_artist_id_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.rank().over(w_release_date_month_artist_id_2).alias("duration_sec_rank"),
        F.round(F.cume_dist().over(w_release_date_month_artist_id_3), 3).alias("duration_sec_cume_dist"),
        F.last("bpm").over(w_release_date_month_artist_id_4).alias("last_bpm"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist_2"),
    )
)

ranked = (
    metrics
    .select(
        "id",
        "artist_id",
        "bpm_max_6_prior",
        "duration_sec_rank",
        "last_bpm",
        "duration_sec_cume_dist_2",
        F.round(F.cume_dist().over(w_all_by_duration_sec_cume_dis), 3).alias("duration_sec_cume_dist_cume_dist"),
        F.row_number().over(w_all_by_duration_sec_cume_dis_2).alias("duration_sec_cume_dist_seq"),
        F.max("duration_sec_cume_dist").over(w_duration_sec_rank_artist_id).alias("duration_sec_rank_artist_id_max_duration_sec_cum"),
        (F.col("prev3_bpm") - F.lag("prev3_bpm").over(w_artist_id_duration_sec_rank)).alias("prev3_bpm_change"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "last_bpm",
        "duration_sec_cume_dist_cume_dist",
        "duration_sec_cume_dist_seq",
        "prev3_bpm_change",
        F.least(F.col("duration_sec_cume_dist_2"), F.col("last_bpm")).alias("least_duration_sec_cume_dist_2_last_bpm"),
        F.least(F.col("prev3_bpm_change"), F.col("duration_sec_cume_dist_2")).alias("least_prev3_bpm_change_duration_sec_cume_dist_2"),
    )
    .orderBy(F.col("last_bpm").desc())
)
