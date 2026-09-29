from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-1, Window.currentRow)
w_release_date_month_artist_id_2 = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_artist_id_3 = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, -1)
w_release_date_month_artist_id_4 = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("bpm").desc(), "id")
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date")).orderBy("bpm", "id")
w_release_date_month_artist_id_5 = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_bpm_prior_6 = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, -1)
w_all_by_total_next_duration_s = Window.orderBy(F.col("total_next_duration_sec").desc(), "is_explicit")
w_all_by_total_next_duration_s_2 = Window.orderBy(F.col("total_next_duration_sec").desc(), "is_explicit").rowsBetween(-6, -1)
w_all_by_total_next_duration_s_3 = Window.orderBy(F.col("total_next_duration_sec").desc(), "is_explicit").rowsBetween(-1, Window.currentRow)

filtered = (
    df_tracks
    .filter((F.col("bpm") > 76) & (F.col("release_date") < F.lit("2025-08-28").cast("date")))
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

with_trends = (
    filtered
    .select(
        "*",
        F.sum("duration_sec").over(w_is_explicit).alias("is_explicit_sum_duration_sec"),
        F.max("bpm").over(w_release_date_month_artist_id).alias("bpm_max_2"),
        F.max("bpm").over(w_release_date_month_artist_id_2).alias("max_bpm_so_far"),
        F.max("duration_sec").over(w_is_explicit).alias("is_explicit_max_duration_sec"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.sum("bpm").over(w_release_date_month_artist_id_3).alias("bpm_sum_6_prior"),
        F.round(F.percent_rank().over(w_release_date_month_artist_id_4), 3).alias("bpm_percent_rank"),
        F.lead("bpm", 3).over(w_all_by_bpm).alias("next3_bpm"),
        (F.col("bpm") - F.lag("bpm").over(w_is_explicit_release_date_mon)).alias("bpm_change"),
        F.max("duration_sec").over(w_release_date_month_artist_id_5).alias("duration_sec_max_3"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit), 2).alias("bpm_vs_is_explicit_avg"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_bpm)).alias("days_since_prev_release_date"),
        F.lead("duration_sec").over(w_all_by_bpm).alias("next_duration_sec"),
        F.max("duration_sec").over(w_all_by_bpm_prior_6).alias("duration_sec_max_6_prior"),
    )
)

rollup_base = (
    with_trends
    .groupBy("is_explicit")
    .agg(
        F.sum("next_duration_sec").alias("total_next_duration_sec"),
        F.sum("bpm").alias("total_bpm"),
        F.max("next_duration_sec").alias("max_next_duration_sec"),
    )
)

with_windows = (
    rollup_base
    .select(
        "is_explicit",
        "total_next_duration_sec",
        "total_bpm",
        F.lead("max_next_duration_sec", 2, 0).over(w_all_by_total_next_duration_s).alias("next2_max_next_duration_sec"),
        F.round(F.avg("total_next_duration_sec").over(w_all_by_total_next_duration_s_2), 2).alias("total_next_duration_sec_avg_6_prior"),
        F.row_number().over(w_all_by_total_next_duration_s).alias("seq_no"),
        F.sum("total_next_duration_sec").over(w_all_by_total_next_duration_s_3).alias("total_next_duration_sec_sum_2"),
    )
)

result = (
    with_windows
    .select(
        "is_explicit",
        "total_next_duration_sec",
        "total_next_duration_sec_avg_6_prior",
        "seq_no",
        "total_next_duration_sec_sum_2",
    )
    .orderBy(
        F.col("total_next_duration_sec_avg_6_prior").desc_nulls_first(),
        F.col("total_next_duration_sec_sum_2").desc(),
        "is_explicit",
    )
    .limit(5)
)
