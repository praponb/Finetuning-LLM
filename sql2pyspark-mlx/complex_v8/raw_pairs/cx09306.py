from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm = Window.orderBy("bpm", "id")
w_all_by_bpm_running = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_duration_sec_pe = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_per_bpm").desc(), "id")
w_all_by_bpm_2 = Window.orderBy(F.col("bpm").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm_running_2 = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_by_duration_sec_pe_2 = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_per_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm_prior_1 = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(-1, -1)
w_rn_by_rows_in_is_explicit = Window.partitionBy("rn").orderBy("rows_in_is_explicit", "id")
w_rn_last_known_release_date_m = Window.partitionBy("rn", F.date_trunc("month", "last_known_release_date")).orderBy("duration_sec_per_bpm_avg_1_prior", "id")
w_rn_last_known_release_date_m_2 = Window.partitionBy("rn", F.date_trunc("month", "last_known_release_date")).orderBy("duration_sec_per_bpm_avg_1_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn_last_known_release_date_m_3 = Window.partitionBy("rn", F.date_trunc("month", "last_known_release_date")).orderBy("duration_sec_per_bpm_avg_1_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
    )
)

metrics = (
    base_tracks
    .select(
        "*",
        F.lag("duration_sec_per_bpm").over(w_all_by_bpm).alias("prev_duration_sec_per_bpm"),
        F.last("release_date", ignorenulls=True).over(w_all_by_bpm_running).alias("last_known_release_date"),
        F.lead("release_date").over(w_artist_id_by_duration_sec_pe).alias("next_release_date"),
        F.round(F.percent_rank().over(w_all_by_bpm_2), 3).alias("bpm_percent_rank"),
        F.row_number().over(w_all_by_bpm_2).alias("rn"),
        F.round(F.col("duration_sec_per_bpm") / F.nullif(F.sum("duration_sec_per_bpm").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_per_bpm_share_of_artist_id"),
        F.first("release_date", ignorenulls=True).over(w_all_by_bpm_running_2).alias("first_known_release_date"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.round(F.cume_dist().over(w_all_by_bpm), 3).alias("bpm_cume_dist"),
        F.round(F.avg("duration_sec_per_bpm").over(w_artist_id), 2).alias("artist_id_avg_duration_sec_per_bpm"),
        F.min("duration_sec_per_bpm").over(w_artist_id_by_duration_sec_pe_2).alias("min_duration_sec_per_bpm_so_far"),
        F.sum("bpm").over(w_artist_id_is_explicit_by_rel).alias("running_bpm"),
        F.round(F.avg("duration_sec_per_bpm").over(w_all_by_bpm_prior_1), 2).alias("duration_sec_per_bpm_avg_1_prior"),
        F.lead("bpm").over(w_all_by_bpm_2).alias("next_bpm"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.ntile(3).over(w_rn_by_rows_in_is_explicit).alias("rows_in_is_explicit_tercile"),
        F.round(F.percent_rank().over(w_rn_last_known_release_date_m), 3).alias("duration_sec_per_bpm_avg_1_prior_percent_rank"),
        F.nth_value("first_known_release_date", 2).over(w_rn_last_known_release_date_m_2).alias("second_first_known_release_date"),
        F.max("duration_sec_per_bpm").over(w_rn_last_known_release_date_m_3).alias("max_duration_sec_per_bpm_so_far"),
    )
)

result = (
    ranked
    .filter(F.col("max_duration_sec_per_bpm_so_far") > 35.0)
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        "duration_sec_per_bpm",
        "release_date_week",
        "prev_duration_sec_per_bpm",
        "last_known_release_date",
        "next_release_date",
        "rn",
        "first_known_release_date",
        "rows_in_is_explicit",
        "artist_id_max_bpm",
        "bpm_cume_dist",
        "min_duration_sec_per_bpm_so_far",
        "running_bpm",
        "duration_sec_per_bpm_avg_1_prior",
        "next_bpm",
        "rows_in_is_explicit_tercile",
        "duration_sec_per_bpm_avg_1_prior_percent_rank",
        "second_first_known_release_date",
        "max_duration_sec_per_bpm_so_far",
        (F.col("rows_in_is_explicit") - F.col("bpm")).alias("rows_in_is_explicit_minus_bpm"),
    )
    .orderBy(F.col("second_first_known_release_date").asc())
)
