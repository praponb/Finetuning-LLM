from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm_ties = Window.partitionBy("is_explicit").orderBy("bpm")
w_all_by_duration_sec_per_bpm = Window.orderBy(F.col("duration_sec_per_bpm").desc_nulls_first(), "id")
w_artist_id_by_duration_sec_pe = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_per_bpm").asc_nulls_last(), "id")
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_all_by_duration_sec_per_bpm_2 = Window.orderBy(F.col("duration_sec_per_bpm").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_all_by_duration_sec_per_bpm_3 = Window.orderBy("duration_sec_per_bpm", "id")
w_moods_count_is_explicit_by_r = Window.partitionBy("moods_count", "is_explicit").orderBy("release_date", "id")
w_all_by_duration_sec_per_bpm_4 = Window.orderBy("duration_sec_per_bpm")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_id_by_duration_sec_pe_2 = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_per_bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_seq_no_moods_count_change_by = Window.partitionBy("seq_no", "moods_count_change").orderBy(F.col("release_date_year_min_duration_sec").desc(), "id")
w_moods_count_change_seq_no = Window.partitionBy("moods_count_change", "seq_no")
w_all_by_duration_sec_per_bpm_5 = Window.orderBy("duration_sec_per_bpm_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_per_bpm_6 = Window.orderBy("duration_sec_per_bpm_percent_rank", "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy("duration_sec_per_bpm_percent_rank")

scoped = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "workout")) & (F.col("bpm") <= 143))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_per_bpm",
        "moods_count",
        F.round(F.percent_rank().over(w_is_explicit_by_bpm_ties), 3).alias("bpm_percent_rank"),
        F.lag("duration_sec").over(w_all_by_duration_sec_per_bpm).alias("prev_duration_sec"),
        F.row_number().over(w_artist_id_by_duration_sec_pe).alias("seq_no"),
        F.round(F.col("duration_sec_per_bpm") / F.nullif(F.sum("duration_sec_per_bpm").over(w_release_date_year), F.lit(0)), 4).alias("duration_sec_per_bpm_share_of_release_date_year"),
        F.round(F.avg("moods_count").over(w_release_date_year), 2).alias("release_date_year_avg_moods_count"),
        F.round(F.avg("moods_count").over(w_all_by_duration_sec_per_bpm_2), 2).alias("moods_count_avg_6_prior"),
        F.lead("duration_sec", 2).over(w_all_by_duration_sec_per_bpm_3).alias("next2_duration_sec"),
        (F.col("moods_count") - F.lag("moods_count").over(w_moods_count_is_explicit_by_r)).alias("moods_count_change"),
        (F.col("duration_sec_per_bpm") - F.lag("duration_sec_per_bpm").over(w_all_by_duration_sec_per_bpm_3)).alias("duration_sec_per_bpm_change"),
        F.dense_rank().over(w_all_by_duration_sec_per_bpm_4).alias("duration_sec_per_bpm_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_per_bpm_4), 3).alias("duration_sec_per_bpm_percent_rank"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_release_date_year), 2).alias("bpm_vs_release_date_year_avg"),
        F.min("duration_sec").over(w_release_date_year).alias("release_date_year_min_duration_sec"),
        F.first("release_date", ignorenulls=True).over(w_artist_id_by_duration_sec_pe_2).alias("first_known_release_date"),
        F.round(F.avg("moods_count").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_moods_count"),
    )
)

annotated = (
    with_windows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_per_bpm",
        "moods_count",
        "prev_duration_sec",
        "seq_no",
        "release_date_year_avg_moods_count",
        "moods_count_avg_6_prior",
        "next2_duration_sec",
        "moods_count_change",
        "duration_sec_per_bpm_change",
        "duration_sec_per_bpm_dense_rank",
        "duration_sec_per_bpm_percent_rank",
        "bpm_vs_release_date_year_avg",
        "release_date_year_min_duration_sec",
        "first_known_release_date",
        "is_explicit_artist_id_avg_moods_count",
        F.abs(F.col("bpm") - 147).alias("bpm_distance"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.lead("moods_count_avg_6_prior", 2, 0).over(w_seq_no_moods_count_change_by).alias("next2_moods_count_avg_6_prior"),
        F.min("duration_sec").over(w_moods_count_change_seq_no).alias("moods_count_change_seq_no_min_duration_sec"),
        F.round(F.cume_dist().over(w_seq_no_moods_count_change_by), 3).alias("release_date_year_min_duration_sec_cume_dist"),
        F.min("moods_count").over(w_all_by_duration_sec_per_bpm_5).alias("min_moods_count_so_far"),
        F.lead("duration_sec_per_bpm_change", 1, 0).over(w_all_by_duration_sec_per_bpm_6).alias("next_duration_sec_per_bpm_change"),
        F.rank().over(w_is_explicit_by_duration_sec).alias("duration_sec_per_bpm_percent_rank_rank"),
        F.row_number().over(w_all_by_duration_sec_per_bpm_6).alias("seq_no_2"),
        F.round(F.avg("duration_sec_per_bpm").over(w_moods_count_change_seq_no), 2).alias("moods_count_change_seq_no_avg_duration_sec_per_b"),
    )
)

result = (
    metrics
    .select(
        "id",
        "is_explicit",
        "bpm",
        "duration_sec_per_bpm",
        "prev_duration_sec",
        "seq_no",
        "release_date_year_avg_moods_count",
        "moods_count_avg_6_prior",
        "next2_duration_sec",
        "moods_count_change",
        "duration_sec_per_bpm_change",
        "duration_sec_per_bpm_percent_rank",
        "bpm_vs_release_date_year_avg",
        "release_date_year_min_duration_sec",
        "bpm_distance",
        "next2_moods_count_avg_6_prior",
        "release_date_year_min_duration_sec_cume_dist",
        "next_duration_sec_per_bpm_change",
        "duration_sec_per_bpm_percent_rank_rank",
        "seq_no_2",
        F.year(F.col("first_known_release_date")).alias("first_known_release_date_year"),
        F.round(F.col("prev_duration_sec") / F.nullif(F.col("seq_no_2"), F.lit(0)), 3).alias("prev_duration_sec_per_seq_no_2"),
    )
    .orderBy(F.col("bpm").desc_nulls_first())
)
