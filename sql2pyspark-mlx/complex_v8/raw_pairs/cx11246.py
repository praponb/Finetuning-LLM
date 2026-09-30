from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_flag_is_explicit = Window.partitionBy("is_explicit_flag", "is_explicit").orderBy("bpm", "id")
w_release_date_month_is_explic = Window.partitionBy(F.month("release_date"), "is_explicit")
w_artist_id_is_explicit_flag_b = Window.partitionBy("artist_id", "is_explicit_flag").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_all_by_days_since_release_da = Window.orderBy(F.col("days_since_release_date").desc(), "id")
w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id")
w_is_explicit_flag_is_explicit_2 = Window.partitionBy("is_explicit_flag", "is_explicit").orderBy("bpm")
w_all_by_rows_in_release_date = Window.orderBy("rows_in_release_date_month_artist_id", "id")
w_artist_id_bpm_dense_rank_by = Window.partitionBy("artist_id", "bpm_dense_rank").orderBy(F.col("is_explicit_flag").desc())
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date"))
w_bpm_seq_is_explicit_flag_by = Window.partitionBy("bpm_seq", "is_explicit_flag").orderBy("next3_duration_sec", "id")
w_is_explicit_by_next3_duratio = Window.partitionBy("is_explicit").orderBy(F.col("next3_duration_sec").desc(), "id")

base_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (~F.array_contains(F.col("moods"), "sad")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

with_trends = (
    base_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "days_since_release_date",
        "is_explicit_flag",
        F.row_number().over(w_is_explicit_flag_is_explicit).alias("bpm_seq"),
        F.round(F.col("is_explicit_flag") / F.nullif(F.sum("is_explicit_flag").over(w_release_date_month_is_explic), F.lit(0)), 4).alias("is_explicit_flag_share_of_release_date_month_is_"),
        F.ntile(5).over(w_artist_id_is_explicit_flag_b).alias("duration_sec_quintile"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_days_since_release_da)).alias("days_since_prev_release_date"),
        F.round(F.col("is_explicit_flag") / F.nullif(F.sum("is_explicit_flag").over(w_release_date_month_artist_id), F.lit(0)), 4).alias("is_explicit_flag_share_of_release_date_month_art"),
        F.lead("duration_sec", 3).over(w_all_by_days_since_release_da).alias("next3_duration_sec"),
        F.count(F.lit(1)).over(w_release_date_month_artist_id).alias("rows_in_release_date_month_artist_id"),
        F.dense_rank().over(w_is_explicit_flag_is_explicit_2).alias("bpm_dense_rank"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "days_since_release_date",
        "is_explicit_flag",
        "bpm_seq",
        "is_explicit_flag_share_of_release_date_month_is_",
        "duration_sec_quintile",
        "days_since_prev_release_date",
        "is_explicit_flag_share_of_release_date_month_art",
        "next3_duration_sec",
        "rows_in_release_date_month_artist_id",
        F.lag("duration_sec", 3).over(w_all_by_rows_in_release_date).alias("prev3_duration_sec"),
        (F.col("next3_duration_sec") - F.lag("next3_duration_sec").over(w_all_by_rows_in_release_date)).alias("next3_duration_sec_change"),
        F.rank().over(w_artist_id_bpm_dense_rank_by).alias("is_explicit_flag_rank"),
        F.min("bpm").over(w_artist_id_release_date_year).alias("artist_id_release_date_year_min_bpm"),
        F.dense_rank().over(w_bpm_seq_is_explicit_flag_by).alias("next3_duration_sec_dense_rank"),
        F.dense_rank().over(w_is_explicit_by_next3_duratio).alias("next3_duration_sec_dense_rank_2"),
        F.round(F.col("is_explicit_flag") / F.nullif(F.sum("is_explicit_flag").over(w_artist_id_release_date_year), F.lit(0)), 4).alias("is_explicit_flag_share_of_artist_id_release_date"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "is_explicit_flag",
        "bpm_seq",
        "is_explicit_flag_share_of_release_date_month_is_",
        "days_since_prev_release_date",
        "rows_in_release_date_month_artist_id",
        "next3_duration_sec_change",
        "is_explicit_flag_rank",
        "artist_id_release_date_year_min_bpm",
        "next3_duration_sec_dense_rank",
        "next3_duration_sec_dense_rank_2",
        (F.col("duration_sec_quintile") - F.col("duration_sec")).alias("duration_sec_quintile_minus_duration_sec"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)
