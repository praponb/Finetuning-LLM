from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_running = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_all_by_release_date_full = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_ties = Window.orderBy(F.col("release_date").desc())
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy("duration_sec", "id")
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_id = Window.partitionBy("artist_id")
w_release_date_month_is_explic = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy("duration_sec", "id").rowsBetween(-1, Window.currentRow)
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id")
w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")
w_all_by_first_duration_sec = Window.orderBy("first_duration_sec", "id")
w_next2_release_date_month_is = Window.partitionBy(F.date_trunc("month", "next2_release_date"), "is_explicit")
w_all_by_first_duration_sec_ti = Window.orderBy("first_duration_sec")
w_bpm_decile_by_bpm_decile_mov = Window.partitionBy("bpm_decile").orderBy(F.col("bpm_decile").desc(), "id").rowsBetween(-6, Window.currentRow)
w_next2_release_date_month_by = Window.partitionBy(F.date_trunc("month", "next2_release_date")).orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_bpm_decile_by_bpm_decile = Window.partitionBy("bpm_decile").orderBy(F.col("bpm_decile").desc(), "id")

scoped = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.col("bpm") >= 85))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.first("bpm", ignorenulls=True).over(w_all_by_release_date_running).alias("first_known_bpm"),
        F.lag("release_date").over(w_artist_id_by_release_date).alias("prev_release_date"),
        F.last("bpm").over(w_all_by_release_date_full).alias("last_bpm"),
        F.round(F.percent_rank().over(w_all_by_release_date_ties), 3).alias("release_date_percent_rank"),
        F.lead("release_date", 2).over(w_artist_id_release_date_month).alias("next2_release_date"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit_artist_id), 2).alias("duration_sec_vs_is_explicit_artist_id_avg"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.round(F.avg("duration_sec").over(w_release_date_month_is_explic), 2).alias("duration_sec_avg_2"),
        F.ntile(10).over(w_is_explicit_by_bpm).alias("bpm_decile"),
        F.first("duration_sec").over(w_artist_id_by_release_date_fu).alias("first_duration_sec"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.rank().over(w_all_by_release_date).alias("release_date_rank"),
        F.lead("bpm", 3, 0).over(w_all_by_release_date).alias("next3_bpm"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.when(F.col("release_date_rank") >= 8, "large").when(F.col("release_date_rank") >= 5, "medium").otherwise("small").alias("release_date_rank_tier"),
        F.date_format(F.col("prev_release_date"), "yyyy-MM-dd").alias("prev_release_date_label"),
        F.date_format(F.col("prev_release_date"), "yyyy-MM").alias("prev_release_date_label_2"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.lead("bpm").over(w_all_by_first_duration_sec).alias("next_bpm"),
        F.sum("first_duration_sec").over(w_next2_release_date_month_is).alias("next2_release_date_month_is_explicit_sum_first_d"),
        F.round(F.percent_rank().over(w_all_by_first_duration_sec_ti), 3).alias("first_duration_sec_percent_rank"),
        F.ntile(3).over(w_all_by_first_duration_sec).alias("first_duration_sec_tercile"),
        F.max("first_duration_sec").over(w_bpm_decile_by_bpm_decile_mov).alias("first_duration_sec_max_7"),
        F.max("next3_bpm").over(w_next2_release_date_month_by).alias("max_next3_bpm_so_far"),
        F.lag("last_bpm", 2).over(w_bpm_decile_by_bpm_decile).alias("prev2_last_bpm"),
        F.lead("release_date").over(w_bpm_decile_by_bpm_decile).alias("next_release_date"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "title_prefix",
        "first_known_bpm",
        "last_bpm",
        "release_date_percent_rank",
        "duration_sec_cume_dist",
        "duration_sec_vs_is_explicit_artist_id_avg",
        "bpm_vs_artist_id_avg",
        "duration_sec_avg_2",
        "bpm_decile",
        "first_duration_sec",
        "rows_in_is_explicit_artist_id",
        "release_date_rank",
        "prev_release_date_label",
        "prev_release_date_label_2",
        "next2_release_date_month_is_explicit_sum_first_d",
        "first_duration_sec_percent_rank",
        "first_duration_sec_tercile",
        "max_next3_bpm_so_far",
        "prev2_last_bpm",
        "next_release_date",
    )
    .orderBy(F.col("bpm").desc(), F.col("id").desc())
)
