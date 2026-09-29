from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_flag_by_is_expli = Window.partitionBy("is_explicit_flag").orderBy("is_explicit_flag", "id")
w_is_explicit_flag = Window.partitionBy("is_explicit_flag")
w_is_explicit_flag_artist_id = Window.partitionBy("is_explicit_flag", "artist_id")
w_is_explicit_flag_by_is_expli_2 = Window.partitionBy("is_explicit_flag").orderBy("is_explicit_flag", "id").rowsBetween(-2, Window.currentRow)
w_is_explicit_flag_by_is_expli_3 = Window.partitionBy("is_explicit_flag").orderBy("is_explicit_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_flag_artist_id_b = Window.partitionBy("is_explicit_flag", "artist_id").orderBy("is_explicit_flag", "id")
w_is_explicit_flag_by_is_expli_4 = Window.partitionBy("is_explicit_flag").orderBy(F.col("is_explicit_flag").desc(), "id")
w_is_explicit_flag_artist_id_b_2 = Window.partitionBy("is_explicit_flag", "artist_id").orderBy("is_explicit_flag")
w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_flag_decile_by_i = Window.partitionBy("is_explicit_flag_decile").orderBy(F.col("is_explicit_flag_cume_dist_2_to_days_since_prev_").desc(), "is_explicit_flag", "is_explicit_flag_decile")

base = (
    df_tracks
    .filter(F.col("bpm") < 165)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.abs(F.col("duration_sec") - 309).alias("duration_sec_distance"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.ntile(10).over(w_is_explicit_flag_by_is_expli).alias("is_explicit_flag_decile"),
        F.max("duration_sec_distance").over(w_is_explicit_flag).alias("is_explicit_flag_max_duration_sec_distance"),
        F.max("duration_sec_distance").over(w_is_explicit_flag_artist_id).alias("is_explicit_flag_artist_id_max_duration_sec_dist"),
        F.max("duration_sec_distance").over(w_is_explicit_flag_by_is_expli_2).alias("duration_sec_distance_max_3"),
        F.last("duration_sec_distance", ignorenulls=True).over(w_is_explicit_flag_by_is_expli_3).alias("last_known_duration_sec_distance"),
        F.round(F.col("is_explicit_flag") - F.avg("is_explicit_flag").over(w_is_explicit_flag), 2).alias("is_explicit_flag_vs_is_explicit_flag_avg"),
        F.lag("duration_sec_distance", 3).over(w_is_explicit_flag_artist_id_b).alias("prev3_duration_sec_distance"),
        F.round(F.col("is_explicit_flag") / F.nullif(F.sum("is_explicit_flag").over(w_is_explicit_flag_artist_id), F.lit(0)), 4).alias("is_explicit_flag_share_of_is_explicit_flag_artis"),
        F.lag("release_date", 2).over(w_is_explicit_flag_by_is_expli).alias("prev2_release_date"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_flag_artist_id_b)).alias("days_since_prev_release_date"),
        F.round(F.cume_dist().over(w_is_explicit_flag_by_is_expli_4), 3).alias("is_explicit_flag_cume_dist"),
        F.row_number().over(w_is_explicit_flag_by_is_expli_4).alias("is_explicit_flag_seq"),
        F.round(F.cume_dist().over(w_is_explicit_flag_artist_id_b_2), 3).alias("is_explicit_flag_cume_dist_2"),
        F.last("release_date").over(w_artist_id_by_release_date_fu).alias("last_release_date"),
    )
)

rollup_base = (
    with_windows
    .groupBy("is_explicit_flag", "is_explicit_flag_decile")
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("title"))).like("G%"), F.col("is_explicit_flag_share_of_is_explicit_flag_artis")).otherwise(0)).alias("is_explicit_flag_share_of_is_explicit_flag_artis_2"),
        F.round(F.stddev("is_explicit_flag_vs_is_explicit_flag_avg"), 2).alias("stddev_is_explicit_flag_vs_is_explicit_flag_avg"),
        F.round(F.avg("last_known_duration_sec_distance"), 2).alias("avg_last_known_duration_sec_distance"),
        F.round(F.sum("is_explicit_flag_cume_dist_2") / F.nullif(F.sum("days_since_prev_release_date"), F.lit(0)), 3).alias("is_explicit_flag_cume_dist_2_to_days_since_prev_"),
    )
)

ranked = (
    rollup_base
    .select(
        "is_explicit_flag",
        "is_explicit_flag_decile",
        "is_explicit_flag_share_of_is_explicit_flag_artis_2",
        "stddev_is_explicit_flag_vs_is_explicit_flag_avg",
        "avg_last_known_duration_sec_distance",
        "is_explicit_flag_cume_dist_2_to_days_since_prev_",
        F.rank().over(w_is_explicit_flag_decile_by_i).alias("is_explicit_flag_cume_dist_2_to_days_since_prev__2"),
        F.ntile(4).over(w_is_explicit_flag_decile_by_i).alias("is_explicit_flag_cume_dist_2_to_days_since_prev__3"),
    )
)

result = (
    ranked
    .select(
        "is_explicit_flag",
        "is_explicit_flag_decile",
        "stddev_is_explicit_flag_vs_is_explicit_flag_avg",
        "is_explicit_flag_cume_dist_2_to_days_since_prev_",
        "is_explicit_flag_cume_dist_2_to_days_since_prev__3",
        (F.floor(F.col("is_explicit_flag_decile") / 2) * 2).alias("is_explicit_flag_decile_bucket"),
        F.when(F.col("is_explicit_flag") >= 1, "A").when(F.col("is_explicit_flag") >= 0, "B").otherwise("C").alias("is_explicit_flag_tier"),
    )
)
