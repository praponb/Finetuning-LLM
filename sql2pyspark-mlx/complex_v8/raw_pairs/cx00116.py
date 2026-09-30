from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, Window.currentRow)
w_source_played_at_month_by_ms = Window.partitionBy("source", F.date_trunc("month", "played_at")).orderBy("ms_played", "id").rowsBetween(-3, -1)
w_source_played_at_month_by_ms_2 = Window.partitionBy("source", F.date_trunc("month", "played_at")).orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_played_at_month_by_ms_3 = Window.partitionBy("source", F.date_trunc("month", "played_at")).orderBy("ms_played", "id")
w_ms_played_tier_track_id = Window.partitionBy("ms_played_tier", "track_id")
w_device_listener_id_by_ms_pla_2 = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc())
w_all_by_ms_played_tier_track = Window.orderBy(F.col("ms_played_tier_track_id_avg_ms_played_filled").desc(), "id")
w_track_id = Window.partitionBy("track_id")
w_all_by_ms_played_vs_ms_playe = Window.orderBy(F.col("ms_played_vs_ms_played_tier_track_id_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_second_played_at = Window.partitionBy("track_id").orderBy("second_played_at", "id").rowsBetween(-1, Window.currentRow)

base = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.when(F.col("ms_played") >= 299017, "A").when(F.col("ms_played") >= 73335, "B").otherwise("C").alias("ms_played_tier"),
    )
)

windowed = (
    base
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "ms_played_filled",
        "ms_played_tier",
        F.sum("ms_played_filled").over(w_device_listener_id_by_ms_pla).alias("ms_played_filled_sum_7"),
        F.max("ms_played").over(w_source_played_at_month_by_ms).alias("ms_played_max_3_prior"),
        F.nth_value("played_at", 2).over(w_source_played_at_month_by_ms_2).alias("second_played_at"),
        (F.col("ms_played_filled") - F.lag("ms_played_filled").over(w_source_played_at_month_by_ms_3)).alias("ms_played_filled_change"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_ms_played_tier_track_id), 2).alias("ms_played_vs_ms_played_tier_track_id_avg"),
        F.sum("ms_played_filled").over(w_ms_played_tier_track_id).alias("ms_played_tier_track_id_sum_ms_played_filled"),
        F.round(F.avg("ms_played_filled").over(w_ms_played_tier_track_id), 2).alias("ms_played_tier_track_id_avg_ms_played_filled"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_ms_played_tier_track_id), F.lit(0)), 4).alias("ms_played_share_of_ms_played_tier_track_id"),
        F.size(F.collect_set("device").over(w_ms_played_tier_track_id)).alias("distinct_device_in_ms_played_tier_track_id"),
        F.round(F.percent_rank().over(w_device_listener_id_by_ms_pla_2), 3).alias("ms_played_percent_rank"),
        F.round(F.col("ms_played_filled") - F.avg("ms_played_filled").over(w_ms_played_tier_track_id), 2).alias("ms_played_filled_vs_ms_played_tier_track_id_avg"),
        F.round(F.col("ms_played_filled") / F.nullif(F.sum("ms_played_filled").over(w_ms_played_tier_track_id), F.lit(0)), 4).alias("ms_played_filled_share_of_ms_played_tier_track_i"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "ms_played_filled",
        "ms_played_filled_sum_7",
        "ms_played_max_3_prior",
        "second_played_at",
        "ms_played_filled_change",
        "ms_played_vs_ms_played_tier_track_id_avg",
        "ms_played_tier_track_id_sum_ms_played_filled",
        "ms_played_tier_track_id_avg_ms_played_filled",
        "distinct_device_in_ms_played_tier_track_id",
        "ms_played_percent_rank",
        "ms_played_filled_vs_ms_played_tier_track_id_avg",
        "ms_played_filled_share_of_ms_played_tier_track_i",
        F.dense_rank().over(w_all_by_ms_played_tier_track).alias("ms_played_tier_track_id_avg_ms_played_filled_den"),
        F.round(F.col("ms_played_filled") - F.avg("ms_played_filled").over(w_track_id), 2).alias("ms_played_filled_vs_track_id_avg"),
        F.first("ms_played").over(w_all_by_ms_played_vs_ms_playe).alias("first_ms_played"),
        F.round(F.avg("ms_played_filled").over(w_track_id), 2).alias("track_id_avg_ms_played_filled"),
        F.lag("ms_played_percent_rank", 2).over(w_all_by_ms_played_tier_track).alias("prev2_ms_played_percent_rank"),
        F.sum("ms_played_filled_change").over(w_track_id_by_second_played_at).alias("ms_played_filled_change_sum_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "source",
        "ms_played_max_3_prior",
        "ms_played_vs_ms_played_tier_track_id_avg",
        "ms_played_tier_track_id_sum_ms_played_filled",
        "ms_played_tier_track_id_avg_ms_played_filled",
        "distinct_device_in_ms_played_tier_track_id",
        "ms_played_percent_rank",
        "ms_played_filled_vs_ms_played_tier_track_id_avg",
        "ms_played_filled_share_of_ms_played_tier_track_i",
        "ms_played_filled_vs_track_id_avg",
        "first_ms_played",
        "track_id_avg_ms_played_filled",
        "prev2_ms_played_percent_rank",
        F.date_trunc("year", F.col("second_played_at")).alias("second_played_at_year"),
        F.round(F.col("ms_played_percent_rank") * 0.25, 2).alias("ms_played_percent_rank_adj"),
    )
    .orderBy(
        F.col("ms_played_filled_vs_ms_played_tier_track_id_avg").desc_nulls_first(),
        F.col("second_played_at_year").desc(),
    )
)
