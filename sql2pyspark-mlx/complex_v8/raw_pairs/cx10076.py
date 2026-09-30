from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_source_by_played_at = Window.partitionBy("device", "source").orderBy("played_at", "id")
w_ms_played_tier = Window.partitionBy("ms_played_tier")
w_all_by_played_at_moving_1 = Window.orderBy("played_at", "id").rowsBetween(-1, Window.currentRow)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id = Window.partitionBy("track_id")
w_device_by_played_at_running = Window.partitionBy("device").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_vs_ms_playe = Window.orderBy(F.col("ms_played_vs_ms_played_tier_avg").desc())
w_played_at_seq = Window.partitionBy("played_at_seq")
w_played_at_month_device_by_la = Window.partitionBy(F.month("played_at"), "device").orderBy(F.col("last_ms_played").desc(), "id")
w_played_at_month_device_by_la_2 = Window.partitionBy(F.month("played_at"), "device").orderBy(F.col("last_ms_played").desc())
w_all_by_ms_played_vs_ms_playe_2 = Window.orderBy(F.col("ms_played_vs_ms_played_tier_avg").desc(), "id")
w_ms_played_tier_by_total_last = Window.partitionBy("ms_played_tier").orderBy("total_last_ms_played_cume_dist")

recent_plays = (
    df_plays
    .filter((F.col("device") != "tv") | (F.col("listener_id") <= 19))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.when(F.col("ms_played") >= 299017, "large").when(F.col("ms_played") >= 160264, "medium").otherwise("small").alias("ms_played_tier"),
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

sequenced = (
    recent_plays
    .select(
        "*",
        F.row_number().over(w_device_source_by_played_at).alias("played_at_seq"),
        F.round(F.avg("ms_played").over(w_ms_played_tier), 2).alias("ms_played_tier_avg_ms_played"),
        F.round(F.avg("ms_played").over(w_all_by_played_at_moving_1), 2).alias("ms_played_avg_2"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_ms_played_tier), F.lit(0)), 4).alias("ms_played_share_of_ms_played_tier"),
        F.lead("played_at").over(w_all_by_played_at).alias("next_played_at"),
        F.nth_value("played_at", 2).over(w_all_by_played_at_full).alias("second_played_at"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.last("ms_played").over(w_all_by_played_at_full).alias("last_ms_played"),
        F.sum("ms_played").over(w_device_by_played_at_running).alias("running_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_ms_played_tier), 2).alias("ms_played_vs_ms_played_tier_avg"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "ms_played_tier",
        "played_at_seq",
        "ms_played_tier_avg_ms_played",
        "ms_played_avg_2",
        "ms_played_share_of_ms_played_tier",
        "next_played_at",
        "rows_in_track_id",
        "last_ms_played",
        "running_ms_played",
        F.round(F.percent_rank().over(w_all_by_ms_played_vs_ms_playe), 3).alias("ms_played_vs_ms_played_tier_avg_percent_rank"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_played_at_seq), F.lit(0)), 4).alias("ms_played_share_of_played_at_seq"),
        F.lag("rows_in_track_id", 1, 0).over(w_played_at_month_device_by_la).alias("prev_rows_in_track_id"),
        F.round(F.avg("rows_in_track_id").over(w_played_at_seq), 2).alias("played_at_seq_avg_rows_in_track_id"),
        F.round(F.cume_dist().over(w_played_at_month_device_by_la_2), 3).alias("last_ms_played_cume_dist"),
        F.row_number().over(w_all_by_ms_played_vs_ms_playe_2).alias("ms_played_vs_ms_played_tier_avg_seq"),
    )
)

summary = (
    metrics
    .filter(F.col("prev_rows_in_track_id") > 9)
    .groupBy("ms_played_tier")
    .agg(
        F.round(F.sum("last_ms_played_cume_dist"), 2).alias("total_last_ms_played_cume_dist"),
        F.round(F.stddev("last_ms_played_cume_dist"), 2).alias("stddev_last_ms_played_cume_dist"),
        F.max("ms_played_avg_2").alias("max_ms_played_avg_2_check"),
    )
    .filter(F.col("max_ms_played_avg_2_check") > 143250.0)
    .drop("max_ms_played_avg_2_check")
)

windowed = (
    summary
    .select(
        "*",
        F.round(F.percent_rank().over(w_ms_played_tier_by_total_last), 3).alias("total_last_ms_played_cume_dist_percent_rank"),
    )
)

result = (
    windowed
    .filter(F.col("total_last_ms_played_cume_dist_percent_rank") > 0.2)
    .select(
        "ms_played_tier",
        "total_last_ms_played_cume_dist",
        "stddev_last_ms_played_cume_dist",
        "total_last_ms_played_cume_dist_percent_rank",
        F.length(F.trim(F.col("ms_played_tier"))).alias("ms_played_tier_len"),
    )
    .orderBy(
        F.col("ms_played_tier_len").desc(),
        F.col("stddev_last_ms_played_cume_dist").asc_nulls_last(),
        "ms_played_tier",
    )
    .limit(20)
)
