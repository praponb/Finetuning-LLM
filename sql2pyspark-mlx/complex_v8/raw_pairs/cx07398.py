from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date")).orderBy(F.col("duration_sec_minus_bpm").desc())
w_artist_id_by_duration_sec_mi = Window.partitionBy("artist_id").orderBy("duration_sec_minus_bpm")
w_artist_id_by_duration_sec_mi_2 = Window.partitionBy("artist_id").orderBy("duration_sec_minus_bpm", "id")
w_artist_id_by_duration_sec_mi_3 = Window.partitionBy("artist_id").orderBy("duration_sec_minus_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_release_date_mon_2 = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date")).orderBy(F.col("duration_sec_minus_bpm").desc(), "id")
w_all_by_duration_sec_minus_bp = Window.orderBy("duration_sec_minus_bpm_share_of_artist_id_is_exp")
w_bpm_tier_next_release_date_m = Window.partitionBy("bpm_tier", F.date_trunc("month", "next_release_date")).orderBy("second_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_seq_no_by = Window.partitionBy(F.date_trunc("month", "release_date"), "seq_no").orderBy(F.col("bpm_share_of_artist_id_is_explicit").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_bpm_tier_next_release_date_m_2 = Window.partitionBy("bpm_tier", F.date_trunc("month", "next_release_date")).orderBy("second_release_date", "id")

cleaned = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.dense_rank().over(w_is_explicit_release_date_mon).alias("duration_sec_minus_bpm_dense_rank"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("bpm_share_of_artist_id_is_explicit"),
        F.dense_rank().over(w_artist_id_by_duration_sec_mi).alias("duration_sec_minus_bpm_dense_rank_2"),
        F.row_number().over(w_artist_id_by_duration_sec_mi_2).alias("seq_no"),
        F.round(F.col("duration_sec_minus_bpm") / F.nullif(F.sum("duration_sec_minus_bpm").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_minus_bpm_share_of_artist_id_is_exp"),
        F.first("release_date").over(w_artist_id_by_duration_sec_mi_3).alias("first_release_date"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("bpm_vs_artist_id_is_explicit_avg"),
        F.nth_value("release_date", 2).over(w_artist_id_by_duration_sec_mi_3).alias("second_release_date"),
        F.lead("release_date").over(w_is_explicit_release_date_mon_2).alias("next_release_date"),
    )
)

derived = (
    metrics
    .select(
        "*",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("next_release_date")).alias("days_since_next_release_date"),
        F.when(F.col("bpm") >= 127, "A").when(F.col("bpm") >= 89, "B").otherwise("C").alias("bpm_tier"),
    )
)

with_trends = (
    derived
    .select(
        "*",
        F.rank().over(w_all_by_duration_sec_minus_bp).alias("duration_sec_minus_bpm_share_of_artist_id_is_exp_2"),
        F.nth_value("days_since_next_release_date", 2).over(w_bpm_tier_next_release_date_m).alias("second_days_since_next_release_date"),
        F.last("first_release_date").over(w_release_date_month_seq_no_by).alias("last_first_release_date"),
        (F.col("bpm") - F.lag("bpm").over(w_bpm_tier_next_release_date_m_2)).alias("bpm_change"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "rows_in_artist_id_is_explicit",
        "bpm_share_of_artist_id_is_explicit",
        "duration_sec_minus_bpm_dense_rank_2",
        "seq_no",
        "duration_sec_minus_bpm_share_of_artist_id_is_exp",
        "bpm_vs_artist_id_is_explicit_avg",
        "second_release_date",
        "title_len",
        "days_since_next_release_date",
        "bpm_tier",
        "second_days_since_next_release_date",
        "last_first_release_date",
        "bpm_change",
    )
    .orderBy(
        F.col("second_release_date").asc(),
        F.col("bpm_change").desc_nulls_first(),
    )
)
