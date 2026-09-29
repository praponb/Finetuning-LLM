from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_duration_sec_tier = Window.partitionBy("duration_sec_tier")
w_all_by_bpm_running = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_sec_tier_is_explici = Window.partitionBy("duration_sec_tier", "is_explicit")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_duration_sec_tier_artist_id = Window.partitionBy("duration_sec_tier", "artist_id").orderBy("duration_sec_per_bpm")
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_per_bpm = Window.orderBy("duration_sec_per_bpm", "id").rowsBetween(-2, Window.currentRow)
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec_per_bpm_vs_duration_sec_tier_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.when(F.col("duration_sec") >= 357, "large").when(F.col("duration_sec") >= 209, "medium").otherwise("small").alias("duration_sec_tier"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.round(F.col("duration_sec_per_bpm") - F.avg("duration_sec_per_bpm").over(w_duration_sec_tier), 2).alias("duration_sec_per_bpm_vs_duration_sec_tier_avg"),
        F.max("duration_sec_per_bpm").over(w_all_by_bpm_running).alias("max_duration_sec_per_bpm_so_far"),
        F.round(F.col("duration_sec_per_bpm") / F.nullif(F.sum("duration_sec_per_bpm").over(w_duration_sec_tier_is_explici), F.lit(0)), 4).alias("duration_sec_per_bpm_share_of_duration_sec_tier_"),
        F.last("bpm", ignorenulls=True).over(w_all_by_bpm_running).alias("last_known_bpm"),
        F.round(F.cume_dist().over(w_all_by_bpm), 3).alias("bpm_cume_dist"),
        F.round(F.percent_rank().over(w_duration_sec_tier_artist_id), 3).alias("duration_sec_per_bpm_percent_rank"),
        F.count(F.lit(1)).over(w_duration_sec_tier).alias("rows_in_duration_sec_tier"),
        F.nth_value("duration_sec_per_bpm", 2).over(w_all_by_bpm_full).alias("second_duration_sec_per_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_duration_sec_tier), F.lit(0)), 4).alias("bpm_share_of_duration_sec_tier"),
        F.sum("duration_sec_per_bpm").over(w_duration_sec_tier).alias("duration_sec_tier_sum_duration_sec_per_bpm"),
        F.max("bpm").over(w_all_by_duration_sec_per_bpm).alias("bpm_max_3"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        (F.col("rows_in_duration_sec_tier") - F.lag("rows_in_duration_sec_tier").over(w_all_by_bpm)).alias("rows_in_duration_sec_tier_change"),
        F.round(F.avg("rows_in_duration_sec_tier").over(w_duration_sec_tier_is_explici), 2).alias("duration_sec_tier_is_explicit_avg_rows_in_durati"),
        F.last("last_known_bpm", ignorenulls=True).over(w_is_explicit_by_duration_sec).alias("last_known_last_known_bpm"),
    )
)

result = (
    metrics_2
    .filter(F.col("duration_sec_tier_is_explicit_avg_rows_in_durati").between(1.9, 19.1))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "duration_sec_per_bpm",
        "duration_sec_tier",
        "max_duration_sec_per_bpm_so_far",
        "duration_sec_per_bpm_share_of_duration_sec_tier_",
        "last_known_bpm",
        "bpm_cume_dist",
        "second_duration_sec_per_bpm",
        "duration_sec_tier_sum_duration_sec_per_bpm",
        "bpm_max_3",
        "rows_in_duration_sec_tier_change",
        "duration_sec_tier_is_explicit_avg_rows_in_durati",
        "last_known_last_known_bpm",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
    .orderBy(F.col("is_explicit_flag").asc_nulls_last(), "id")
    .limit(5)
)
