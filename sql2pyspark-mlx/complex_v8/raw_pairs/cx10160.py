from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id")
w_all_by_duration_sec_full = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_bpm_decile_is_explicit = Window.partitionBy("bpm_decile", "is_explicit")
w_second_release_date_month_re = Window.partitionBy(F.date_trunc("month", "second_release_date"), F.month("release_date"))
w_all_by_running_count_running = Window.orderBy(F.col("running_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_rows_in_artist_id_run = Window.orderBy("rows_in_artist_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tracks
    .filter(F.col("bpm") < 166)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.row_number().over(w_artist_id_is_explicit_by_bpm).alias("bpm_seq"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_is_explicit"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist"),
        F.ntile(10).over(w_artist_id_is_explicit_by_bpm).alias("bpm_decile"),
        F.round(F.cume_dist().over(w_is_explicit_artist_id_by_rel), 3).alias("release_date_cume_dist"),
        F.nth_value("release_date", 2).over(w_all_by_duration_sec_full).alias("second_release_date"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_duration_sec)).alias("days_since_prev_release_date"),
        F.lead("bpm").over(w_is_explicit_by_release_date).alias("next_bpm"),
        F.count(F.lit(1)).over(w_all_by_duration_sec_running).alias("running_count"),
        F.sum("duration_sec").over(w_artist_id).alias("artist_id_sum_duration_sec"),
        F.round(F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("artist_id_is_explicit_avg_bpm"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
    )
)

leaders = (
    metrics
    .filter((F.col("bpm_seq") <= 3) & (F.col("second_release_date") < F.lit("2025-04-02").cast("date")))
    .drop("bpm_seq")
)

metrics_2 = (
    leaders
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm_vs_artist_id_avg",
        "release_date_cume_dist",
        "second_release_date",
        "days_since_prev_release_date",
        "next_bpm",
        "artist_id_sum_duration_sec",
        "rows_in_artist_id",
        F.round(F.col("duration_sec_cume_dist") / F.nullif(F.sum("duration_sec_cume_dist").over(w_bpm_decile_is_explicit), F.lit(0)), 4).alias("duration_sec_cume_dist_share_of_bpm_decile_is_ex"),
        F.round(F.col("next_bpm") - F.avg("next_bpm").over(w_second_release_date_month_re), 2).alias("next_bpm_vs_second_release_date_month_release_da"),
        F.min("bpm_vs_artist_id_avg").over(w_all_by_running_count_running).alias("min_bpm_vs_artist_id_avg_so_far"),
        F.last("next_bpm", ignorenulls=True).over(w_all_by_rows_in_artist_id_run).alias("last_known_next_bpm"),
    )
)

stats = (
    metrics_2
    .groupBy(
        F.date_trunc("month", "release_date").alias("release_date_month"),
        F.when(F.col("rows_in_artist_id") >= 14, "large").when(F.col("rows_in_artist_id") >= 8, "medium").otherwise("small").alias("rows_in_artist_id_tier"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.avg("next_bpm"), 2).alias("avg_next_bpm"),
        F.min("days_since_prev_release_date").alias("min_days_since_prev_release_date"),
    )
)

result = (
    stats
    .filter(F.col("total_duration_sec") > 698)
    .select(
        "release_date_month",
        "rows_in_artist_id_tier",
        "min_days_since_prev_release_date",
        F.length(F.trim(F.col("rows_in_artist_id_tier"))).alias("rows_in_artist_id_tier_len"),
        F.concat(F.col("rows_in_artist_id_tier"), F.lit("#"), F.col("total_duration_sec").cast("string")).alias("rows_in_artist_id_tier_ref"),
    )
    .orderBy(F.col("rows_in_artist_id_tier_ref").desc())
)
