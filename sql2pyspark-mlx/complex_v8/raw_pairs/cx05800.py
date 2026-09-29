from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc(), "id")
w_all_by_least_bpm_duration_se = Window.orderBy("least_bpm_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date", "id")
w_all_by_least_bpm_duration_se_2 = Window.orderBy("least_bpm_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.month("release_date"))
w_all_by_least_bpm_duration_se_3 = Window.orderBy("least_bpm_duration_sec", "id")
w_seq_no = Window.partitionBy("seq_no")
w_distinct_artist_id_in_is_exp = Window.partitionBy("distinct_artist_id_in_is_explicit_release_date_m", "artist_id").orderBy("distinct_artist_id_in_is_explicit_release_date_m", "id")
w_is_explicit_seq_no_by_rn_run = Window.partitionBy("is_explicit", "seq_no").orderBy(F.col("rn").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_is_explicit_by_seq_no = Window.partitionBy("rn", "is_explicit").orderBy("seq_no", "id")

cleaned = (
    df_tracks
    .filter((F.col("bpm") < 147) | (~F.array_contains(F.col("moods"), "party")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

windowed = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm_minus_duration_sec",
        "least_bpm_duration_sec",
        F.lead("bpm_minus_duration_sec", 2).over(w_is_explicit_artist_id_by_rel).alias("next2_bpm_minus_duration_sec"),
        F.nth_value("least_bpm_duration_sec", 2).over(w_all_by_least_bpm_duration_se).alias("second_least_bpm_duration_sec"),
        F.row_number().over(w_artist_id_is_explicit_by_rel).alias("seq_no"),
        F.max("bpm_minus_duration_sec").over(w_all_by_least_bpm_duration_se_2).alias("max_bpm_minus_duration_sec_so_far"),
        F.size(F.collect_set("artist_id").over(w_is_explicit_release_date_mon)).alias("distinct_artist_id_in_is_explicit_release_date_m"),
        F.row_number().over(w_all_by_least_bpm_duration_se_3).alias("rn"),
        F.lead("release_date", 3).over(w_is_explicit_artist_id_by_rel).alias("next3_release_date"),
        F.lag("bpm_minus_duration_sec").over(w_artist_id_is_explicit_by_rel).alias("prev_bpm_minus_duration_sec"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.count(F.lit(1)).over(w_seq_no).alias("rows_in_seq_no"),
        (F.col("next2_bpm_minus_duration_sec") - F.lag("next2_bpm_minus_duration_sec").over(w_distinct_artist_id_in_is_exp)).alias("next2_bpm_minus_duration_sec_change"),
        F.max("next2_bpm_minus_duration_sec").over(w_is_explicit_seq_no_by_rn_run).alias("max_next2_bpm_minus_duration_sec_so_far"),
        F.size(F.collect_set("artist_id").over(w_seq_no)).alias("distinct_artist_id_in_seq_no"),
        F.round(F.percent_rank().over(w_rn_is_explicit_by_seq_no), 3).alias("seq_no_percent_rank"),
        F.lag("prev_bpm_minus_duration_sec").over(w_rn_is_explicit_by_seq_no).alias("prev_prev_bpm_minus_duration_sec"),
        F.round(F.col("next2_bpm_minus_duration_sec") - F.avg("next2_bpm_minus_duration_sec").over(w_seq_no), 2).alias("next2_bpm_minus_duration_sec_vs_seq_no_avg"),
    )
)

totals = (
    ranked
    .groupBy("is_explicit")
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
    )
)

result = (
    totals
    .select("is_explicit")
    .distinct()
)
