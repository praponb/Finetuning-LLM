from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm = Window.orderBy(F.col("bpm").asc_nulls_last(), "id")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_id_by_duration_sec_ru = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm_ties = Window.orderBy(F.col("bpm").asc_nulls_last())
w_rn = Window.partitionBy("rn")
w_is_explicit_bpm_rank_by_dura = Window.partitionBy("is_explicit", "bpm_rank").orderBy(F.col("duration_sec_vs_artist_id_avg").desc())
w_is_explicit_bpm_rank_by_dura_2 = Window.partitionBy("is_explicit", "bpm_rank").orderBy(F.col("duration_sec_vs_artist_id_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_bpm_rank_by_dura_3 = Window.partitionBy("is_explicit", "bpm_rank").orderBy(F.col("duration_sec_vs_artist_id_avg").desc(), "id")

filtered = (
    df_tracks
    .filter(F.col("duration_sec") < 402)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

ranked = (
    filtered
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        (F.col("duration_sec") - F.lag("duration_sec").over(w_all_by_bpm)).alias("duration_sec_change"),
        F.row_number().over(w_artist_id_by_bpm).alias("rn"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.round(F.percent_rank().over(w_all_by_bpm), 3).alias("bpm_percent_rank"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_bpm)).alias("duration_sec_change_2"),
        F.sum("bpm").over(w_artist_id_by_duration_sec_ru).alias("running_bpm"),
        F.sum("bpm").over(w_artist_id_is_explicit_by_bpm).alias("running_bpm_2"),
        F.rank().over(w_all_by_bpm_ties).alias("bpm_rank"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.count(F.lit(1)).over(w_rn).alias("rows_in_rn"),
        F.rank().over(w_is_explicit_bpm_rank_by_dura).alias("duration_sec_vs_artist_id_avg_rank"),
        F.round(F.col("duration_sec_change") / F.nullif(F.sum("duration_sec_change").over(w_rn), F.lit(0)), 4).alias("duration_sec_change_share_of_rn"),
        F.nth_value("artist_id_max_bpm", 2).over(w_is_explicit_bpm_rank_by_dura_2).alias("second_artist_id_max_bpm"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_rn), F.lit(0)), 4).alias("duration_sec_share_of_rn"),
        (F.col("bpm_rank") - F.lag("bpm_rank").over(w_is_explicit_bpm_rank_by_dura_3)).alias("bpm_rank_change"),
    )
)

result = (
    metrics
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "duration_sec_change",
        "duration_sec_vs_artist_id_avg",
        "bpm_share_of_artist_id",
        "rows_in_is_explicit_artist_id",
        "bpm_percent_rank",
        "bpm_rank",
        "rows_in_rn",
        "second_artist_id_max_bpm",
        "duration_sec_share_of_rn",
        "bpm_rank_change",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
    .orderBy(F.col("bpm").desc(), F.col("duration_sec").asc())
)
