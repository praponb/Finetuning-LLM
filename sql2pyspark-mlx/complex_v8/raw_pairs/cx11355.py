from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_source_by_played_at = Window.partitionBy("device", "source").orderBy("played_at", "id")
w_source = Window.partitionBy("source")
w_played_at_month_is_skipped_b = Window.partitionBy(F.month("played_at"), "is_skipped").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_played_at_ful = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc_nulls_first(), "id")
w_device_source_by_played_at_f = Window.partitionBy("device", "source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_device_len = Window.orderBy("device_len", "id")
w_device_len = Window.partitionBy("device_len")
w_next_played_at_month_played = Window.partitionBy("next_played_at_month", F.year("played_at")).orderBy(F.col("next_played_at_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next_played_at_month_played_2 = Window.partitionBy("next_played_at_month", F.year("played_at")).orderBy(F.col("next_played_at_month").desc(), "id")
w_all_by_device_len_running = Window.orderBy("device_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_next_played_at_mont = Window.partitionBy("track_id", "next_played_at_month")

prepared_plays = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")) | (F.col("source") == "album"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_trends = (
    prepared_plays
    .select(
        "*",
        F.lead("played_at").over(w_device_source_by_played_at).alias("next_played_at"),
        F.min("ms_played").over(w_source).alias("source_min_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2).alias("ms_played_vs_source_avg"),
        F.nth_value("ms_played", 2).over(w_played_at_month_is_skipped_b).alias("second_ms_played"),
        F.first("ms_played").over(w_listener_id_by_played_at_ful).alias("first_ms_played"),
        F.lead("ms_played").over(w_device_source_by_played_at).alias("next_ms_played"),
        F.lag("ms_played", 1, 0).over(w_listener_id_by_played_at).alias("prev_ms_played"),
        F.nth_value("played_at", 2).over(w_device_source_by_played_at_f).alias("second_played_at"),
        F.round(F.percent_rank().over(w_listener_id_by_played_at), 3).alias("played_at_percent_rank"),
    )
)

derived = (
    with_trends
    .filter(F.col("ms_played_vs_source_avg") >= -200000.0)
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source_min_ms_played",
        "ms_played_vs_source_avg",
        "second_ms_played",
        "next_ms_played",
        F.round(F.col("ms_played_vs_source_avg") * 1.07, 2).alias("ms_played_vs_source_avg_adj"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.month(F.col("next_played_at")).alias("next_played_at_month"),
    )
)

ranked = (
    derived
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played_vs_source_avg",
        "second_ms_played",
        "ms_played_vs_source_avg_adj",
        "next_played_at_month",
        F.round(F.percent_rank().over(w_all_by_device_len), 3).alias("device_len_percent_rank"),
        F.round(F.col("next_ms_played") / F.nullif(F.sum("next_ms_played").over(w_device_len), F.lit(0)), 4).alias("next_ms_played_share_of_device_len"),
        F.nth_value("device_len", 2).over(w_next_played_at_month_played).alias("second_device_len"),
        F.size(F.collect_set("track_id").over(w_device_len)).alias("distinct_track_id_in_device_len"),
        F.round(F.cume_dist().over(w_next_played_at_month_played_2), 3).alias("next_played_at_month_cume_dist"),
        F.sum("source_min_ms_played").over(w_all_by_device_len_running).alias("running_source_min_ms_played"),
        F.count(F.lit(1)).over(w_device_len).alias("rows_in_device_len"),
        F.last("ms_played_vs_source_avg", ignorenulls=True).over(w_all_by_device_len_running).alias("last_known_ms_played_vs_source_avg"),
    )
)

per_group = (
    ranked
    .groupBy("next_played_at_month", "track_id")
    .agg(
        F.round(F.stddev("ms_played_vs_source_avg_adj"), 2).alias("stddev_ms_played_vs_source_avg_adj"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("device_len_percent_rank"), 2).alias("stddev_device_len_percent_rank"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

with_trends_2 = (
    per_group
    .select(
        "next_played_at_month",
        "track_id",
        "distinct_ids",
        "stddev_device_len_percent_rank",
        F.round(F.avg("distinct_ids").over(w_track_id_next_played_at_mont), 2).alias("track_id_next_played_at_month_avg_distinct_ids"),
        F.max("next_played_at_month").over(w_track_id_next_played_at_mont).alias("track_id_next_played_at_month_max_next_played_at"),
    )
)

result = (
    with_trends_2
    .select(
        "next_played_at_month",
        "track_id",
        "distinct_ids",
        "stddev_device_len_percent_rank",
        "track_id_next_played_at_month_avg_distinct_ids",
        "track_id_next_played_at_month_max_next_played_at",
        (F.col("next_played_at_month") - F.col("distinct_ids")).alias("next_played_at_month_minus_distinct_ids"),
    )
    .orderBy(
        F.col("next_played_at_month_minus_distinct_ids").desc(),
        F.col("track_id").desc(),
        "next_played_at_month",
    )
    .limit(10)
)
