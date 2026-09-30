from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_release_date_year_by_duratio = Window.partitionBy(F.year("release_date")).orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first())
w_release_date_year_by_duratio_2 = Window.partitionBy(F.year("release_date")).orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(-3, -1)
w_is_explicit = Window.partitionBy("is_explicit")
w_release_date_year_by_duratio_3 = Window.partitionBy(F.year("release_date")).orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(-6, Window.currentRow)
w_duration_sec_decile_seq_no_b = Window.partitionBy("duration_sec_decile", "seq_no").orderBy("next_duration_sec")
w_artist_id = Window.partitionBy("artist_id")
w_release_date_year_duration_s = Window.partitionBy(F.year("release_date"), "duration_sec_decile").orderBy(F.col("duration_sec_decile").desc(), "id")

base_tracks = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2025-08-07").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

windowed = (
    base_tracks
    .select(
        "*",
        F.row_number().over(w_artist_id_by_duration_sec).alias("seq_no"),
        F.nth_value("bpm", 2).over(w_release_date_year_by_duratio).alias("second_bpm"),
        F.round(F.percent_rank().over(w_artist_id_by_duration_sec_ti), 3).alias("duration_sec_percent_rank"),
        F.lead("duration_sec", 1, 0).over(w_release_date_year_by_duratio_2).alias("next_duration_sec"),
        F.round(F.avg("bpm").over(w_is_explicit_by_release_date), 2).alias("bpm_avg_3_prior"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit), 2).alias("duration_sec_vs_is_explicit_avg"),
        F.max("bpm").over(w_release_date_year_by_duratio_3).alias("max_bpm_so_far"),
        F.round(F.avg("duration_sec").over(w_is_explicit), 2).alias("is_explicit_avg_duration_sec"),
        F.ntile(3).over(w_artist_id_by_duration_sec).alias("duration_sec_tercile"),
        F.max("duration_sec").over(w_is_explicit).alias("is_explicit_max_duration_sec"),
        F.ntile(10).over(w_release_date_year_by_duratio_2).alias("duration_sec_decile"),
        F.first("duration_sec").over(w_release_date_year_by_duratio).alias("first_duration_sec"),
        F.lead("release_date", 2).over(w_artist_id_by_duration_sec).alias("next2_release_date"),
        F.sum("bpm").over(w_is_explicit_by_release_date_2).alias("bpm_sum_7"),
    )
)

metrics = (
    windowed
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "title_prefix",
        "seq_no",
        "second_bpm",
        "duration_sec_percent_rank",
        "next_duration_sec",
        "bpm_avg_3_prior",
        "distinct_artist_id_in_is_explicit",
        "duration_sec_vs_is_explicit_avg",
        "max_bpm_so_far",
        "duration_sec_tercile",
        "is_explicit_max_duration_sec",
        "duration_sec_decile",
        "first_duration_sec",
        "next2_release_date",
        "bpm_sum_7",
        F.rank().over(w_duration_sec_decile_seq_no_b).alias("next_duration_sec_rank"),
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.lead("bpm_sum_7", 2).over(w_release_date_year_duration_s).alias("next2_bpm_sum_7"),
    )
)

result = (
    metrics
    .filter(F.col("is_explicit_max_duration_sec") < 2556)
    .select(
        "id",
        "title",
        "duration_sec",
        "bpm",
        "seq_no",
        "duration_sec_percent_rank",
        "next_duration_sec",
        "bpm_avg_3_prior",
        "distinct_artist_id_in_is_explicit",
        "duration_sec_vs_is_explicit_avg",
        "max_bpm_so_far",
        "is_explicit_max_duration_sec",
        "duration_sec_decile",
        "first_duration_sec",
        "next2_release_date",
        "next_duration_sec_rank",
        "artist_id_min_duration_sec",
        "next2_bpm_sum_7",
        F.least(F.col("next_duration_sec"), F.col("next2_bpm_sum_7")).alias("least_next_duration_sec_next2_bpm_sum_7"),
        (F.col("next2_bpm_sum_7") - F.col("next_duration_sec_rank")).alias("next2_bpm_sum_7_minus_next_duration_sec_rank"),
    )
    .orderBy("duration_sec_decile")
)
