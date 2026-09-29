from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm_ties = Window.orderBy("bpm")
w_artist_id_is_explicit_by_dur_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-2, -1)
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id")
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_artist_id_by_bpm_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit_by_dur_3 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec_vs_is_explicit_artist_id_avg").desc())
w_artist_id_seq_no = Window.partitionBy("artist_id", "seq_no")
w_artist_id_by_seq_no_full = Window.partitionBy("artist_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_seq_no_ties = Window.partitionBy("artist_id").orderBy("seq_no")

base = (
    df_tracks
    .filter((F.col("bpm") > 76) & (F.col("release_date") < F.lit("2025-08-02").cast("date")))
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

windowed = (
    base
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit), 2).alias("duration_sec_vs_is_explicit_avg"),
        F.last("bpm", ignorenulls=True).over(w_artist_id_is_explicit_by_dur).alias("last_known_bpm"),
        F.round(F.cume_dist().over(w_all_by_bpm_ties), 3).alias("bpm_cume_dist"),
        F.round(F.avg("duration_sec").over(w_artist_id_is_explicit_by_dur_2), 2).alias("duration_sec_avg_2_prior"),
        F.row_number().over(w_is_explicit_artist_id_by_bpm).alias("seq_no"),
        F.last("duration_sec").over(w_all_by_duration_sec_full).alias("last_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit_artist_id), 2).alias("duration_sec_vs_is_explicit_artist_id_avg"),
        F.first("duration_sec").over(w_is_explicit_artist_id_by_bpm_2).alias("first_duration_sec"),
        F.round(F.cume_dist().over(w_artist_id_is_explicit_by_dur_3), 3).alias("duration_sec_cume_dist"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit), F.lit(0)), 4).alias("bpm_share_of_is_explicit"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.round(F.cume_dist().over(w_is_explicit_by_duration_sec), 3).alias("duration_sec_vs_is_explicit_artist_id_avg_cume_d"),
        F.round(F.col("duration_sec_avg_2_prior") - F.avg("duration_sec_avg_2_prior").over(w_artist_id_seq_no), 2).alias("duration_sec_avg_2_prior_vs_artist_id_seq_no_avg"),
        F.rank().over(w_is_explicit_by_duration_sec).alias("duration_sec_vs_is_explicit_artist_id_avg_rank"),
        F.nth_value("bpm_cume_dist", 2).over(w_artist_id_by_seq_no_full).alias("second_bpm_cume_dist"),
        F.dense_rank().over(w_artist_id_by_seq_no_ties).alias("seq_no_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_vs_is_explicit_avg",
        "last_known_bpm",
        "bpm_cume_dist",
        "duration_sec_avg_2_prior",
        "last_duration_sec",
        "duration_sec_vs_is_explicit_artist_id_avg",
        "duration_sec_cume_dist",
        "bpm_share_of_is_explicit",
        "duration_sec_vs_is_explicit_artist_id_avg_cume_d",
        "duration_sec_vs_is_explicit_artist_id_avg_rank",
        "second_bpm_cume_dist",
        "seq_no_dense_rank",
        F.least(F.col("duration_sec_cume_dist"), F.col("duration_sec_avg_2_prior")).alias("least_duration_sec_cume_dist_duration_sec_avg_2_"),
    )
    .orderBy(
        F.col("least_duration_sec_cume_dist_duration_sec_avg_2_").desc_nulls_first(),
        "seq_no_dense_rank",
        "id",
    )
    .limit(10)
)
