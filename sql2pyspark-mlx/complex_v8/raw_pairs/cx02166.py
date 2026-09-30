from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, -1)
w_is_explicit_by_duration_sec_3 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_moving_1 = Window.orderBy("duration_sec", "id").rowsBetween(-1, Window.currentRow)
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_bpm_vs_artist_id_is_e = Window.orderBy("bpm_vs_artist_id_is_explicit_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_vs_artist_id_is_e_2 = Window.orderBy("bpm_vs_artist_id_is_explicit_avg", "id")

scoped = (
    df_tracks
    .filter(F.col("bpm") > 96)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.lag("bpm", 3, 0).over(w_is_explicit_by_duration_sec).alias("prev3_bpm"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.round(F.avg("duration_sec").over(w_is_explicit_by_duration_sec_2), 2).alias("duration_sec_avg_3_prior"),
        F.first("bpm").over(w_is_explicit_by_duration_sec_3).alias("first_bpm"),
        F.round(F.avg("duration_sec").over(w_all_by_duration_sec_moving_1), 2).alias("duration_sec_avg_2"),
        F.min("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_min_bpm"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg"),
        F.ntile(5).over(w_artist_id_is_explicit_by_dur).alias("duration_sec_quintile"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("bpm_vs_artist_id_is_explicit_avg"),
        F.first("duration_sec", ignorenulls=True).over(w_all_by_duration_sec_running).alias("first_known_duration_sec"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("bpm_share_of_is_explicit_artist_id"),
        F.row_number().over(w_all_by_duration_sec).alias("seq_no"),
    )
)

top_per_group = (
    with_windows
    .filter((F.col("seq_no") <= 2) & (F.col("first_bpm") < 143))
    .drop("seq_no")
)

metrics = (
    top_per_group
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "title_prefix",
        "prev3_bpm",
        "duration_sec_avg_3_prior",
        "first_bpm",
        "duration_sec_avg_2",
        "is_explicit_artist_id_min_bpm",
        "duration_sec_vs_artist_id_is_explicit_avg",
        "duration_sec_quintile",
        "bpm_vs_artist_id_is_explicit_avg",
        F.nth_value("first_bpm", 2).over(w_all_by_bpm_vs_artist_id_is_e).alias("second_first_bpm"),
        F.sum("duration_sec_quintile").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_sum_duration_sec_quintile"),
        F.nth_value("prev3_bpm", 2).over(w_all_by_bpm_vs_artist_id_is_e).alias("second_prev3_bpm"),
        F.round(F.col("prev3_bpm") - F.avg("prev3_bpm").over(w_artist_id_is_explicit), 2).alias("prev3_bpm_vs_artist_id_is_explicit_avg"),
        F.last("prev3_bpm").over(w_all_by_bpm_vs_artist_id_is_e).alias("last_prev3_bpm"),
        (F.col("first_known_duration_sec") - F.lag("first_known_duration_sec").over(w_all_by_bpm_vs_artist_id_is_e_2)).alias("first_known_duration_sec_change"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg_2"),
        F.round(F.col("duration_sec_avg_2") - F.avg("duration_sec_avg_2").over(w_artist_id_is_explicit), 2).alias("duration_sec_avg_2_vs_artist_id_is_explicit_avg"),
    )
)

stats = (
    metrics
    .filter(F.col("duration_sec_avg_2") <= 387.0)
    .groupBy("artist_id", "duration_sec_quintile", "is_explicit")
    .agg(
        F.round(F.sum("duration_sec_vs_artist_id_is_explicit_avg_2") / F.nullif(F.sum("duration_sec_avg_3_prior"), F.lit(0)), 3).alias("duration_sec_vs_artist_id_is_explicit_avg_2_to_d"),
        F.sum("duration_sec_vs_artist_id_is_explicit_avg").alias("total_duration_sec_vs_artist_id_is_explicit_avg"),
        F.round(F.avg("first_bpm"), 2).alias("avg_first_bpm"),
        F.sum("artist_id_is_explicit_sum_duration_sec_quintile").alias("total_artist_id_is_explicit_sum_duration_sec_qui"),
        F.sum("duration_sec_vs_artist_id_is_explicit_avg_2").alias("total_duration_sec_vs_artist_id_is_explicit_avg_"),
        F.percentile_approx("is_explicit_artist_id_min_bpm", 0.5).alias("median_is_explicit_artist_id_min_bpm"),
        F.max("first_known_duration_sec_change").alias("max_first_known_duration_sec_change_check"),
    )
    .filter(F.col("max_first_known_duration_sec_change_check") > -336)
    .drop("max_first_known_duration_sec_change_check")
)

result = (
    stats
    .select(
        "artist_id",
        "duration_sec_quintile",
        "is_explicit",
        "duration_sec_vs_artist_id_is_explicit_avg_2_to_d",
        "avg_first_bpm",
        "total_artist_id_is_explicit_sum_duration_sec_qui",
        "total_duration_sec_vs_artist_id_is_explicit_avg_",
        "median_is_explicit_artist_id_min_bpm",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
    .orderBy(
        F.col("median_is_explicit_artist_id_min_bpm").asc(),
        F.col("is_explicit_flag").asc(),
    )
)
