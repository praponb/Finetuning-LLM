from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_fu = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_flag_artist_id = Window.partitionBy("is_explicit_flag", "artist_id")
w_is_explicit_flag = Window.partitionBy("is_explicit_flag")
w_all_by_max_duration_sec = Window.orderBy("max_duration_sec", "release_date_ym", "is_explicit_flag")
w_all_by_max_duration_sec_full = Window.orderBy("max_duration_sec", "release_date_ym", "is_explicit_flag").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_tracks
    .filter((~F.array_contains(F.col("moods"), "workout")) & (F.col("duration_sec") < 352))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "is_explicit_flag",
        "title_prefix",
        F.first("duration_sec").over(w_artist_id_by_duration_sec_fu).alias("first_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit_flag_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit_flag_artist_id"),
    )
)

rollup_base = (
    metrics
    .filter(F.upper(F.trim(F.col("title"))).like("G%"))
    .groupBy(
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
        "is_explicit_flag",
    )
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
        F.sum(F.when(F.col("title").isNotNull(), F.col("is_explicit_flag")).otherwise(0)).alias("is_explicit_flag_title_is_not_null"),
        F.round(F.stddev("is_explicit_flag"), 2).alias("stddev_is_explicit_flag"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
)

with_activity = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("stddev_is_explicit_flag").alias("overall_avg_stddev_is_explicit_flag")))
    .filter(F.col("stddev_is_explicit_flag") > F.col("overall_avg_stddev_is_explicit_flag"))
    .drop("overall_avg_stddev_is_explicit_flag")
)

metrics_2 = (
    with_activity
    .select(
        "*",
        F.round(F.col("is_explicit_flag") - F.avg("is_explicit_flag").over(w_is_explicit_flag), 2).alias("is_explicit_flag_vs_is_explicit_flag_avg"),
        F.round(F.col("release_date_span_days") / F.nullif(F.sum("release_date_span_days").over(w_is_explicit_flag), F.lit(0)), 4).alias("release_date_span_days_share_of_is_explicit_flag"),
        F.dense_rank().over(w_all_by_max_duration_sec).alias("max_duration_sec_dense_rank"),
        F.round(F.col("max_duration_sec") - F.avg("max_duration_sec").over(w_is_explicit_flag), 2).alias("max_duration_sec_vs_is_explicit_flag_avg"),
        F.round(F.avg("is_explicit_flag").over(w_is_explicit_flag), 2).alias("is_explicit_flag_avg_is_explicit_flag"),
        F.nth_value("is_explicit_flag", 2).over(w_all_by_max_duration_sec_full).alias("second_is_explicit_flag"),
    )
)

result = (
    metrics_2
    .select(
        "release_date_ym",
        "is_explicit_flag",
        "max_duration_sec",
        "release_date_span_days",
        "is_explicit_flag_title_is_not_null",
        "stddev_is_explicit_flag",
        "is_explicit_flag_vs_is_explicit_flag_avg",
        "release_date_span_days_share_of_is_explicit_flag",
        "max_duration_sec_dense_rank",
        "second_is_explicit_flag",
        F.round(F.col("release_date_span_days") / F.nullif(F.col("is_explicit_flag"), F.lit(0)), 3).alias("release_date_span_days_per_is_explicit_flag"),
    )
    .orderBy(
        F.col("is_explicit_flag_vs_is_explicit_flag_avg").asc(),
        F.col("max_duration_sec_dense_rank").asc_nulls_last(),
    )
)
