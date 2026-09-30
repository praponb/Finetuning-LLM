from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month = Window.partitionBy(F.month("played_at"))
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_track_id_by_played_at_full = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_by_played_at_ties = Window.partitionBy("source").orderBy("played_at")
w_source_by_played_at = Window.partitionBy("source").orderBy("played_at", "id")
w_distinct_ms_played_tier_in_p = Window.partitionBy("distinct_ms_played_tier_in_played_at_month", "listener_id")
w_all_by_next_played_at_full = Window.orderBy(F.col("next_played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_second_played_at_month_by_se = Window.partitionBy(F.month("second_played_at")).orderBy("second_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_ms_played_tier_in_p_2 = Window.partitionBy("distinct_ms_played_tier_in_played_at_month").orderBy(F.col("ms_played_filled").desc(), "id")

valid_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.when(F.col("ms_played") >= 308104, "A").when(F.col("ms_played") >= 73625, "B").otherwise("C").alias("ms_played_tier"),
    )
)

sequenced = (
    valid_plays
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_played_at_month), 2).alias("played_at_month_avg_ms_played"),
        F.lead("played_at").over(w_all_by_ms_played).alias("next_played_at"),
        F.size(F.collect_set("ms_played_tier").over(w_played_at_month)).alias("distinct_ms_played_tier_in_played_at_month"),
        F.count(F.lit(1)).over(w_played_at_month).alias("rows_in_played_at_month"),
        F.nth_value("played_at", 2).over(w_track_id_by_played_at_full).alias("second_played_at"),
        F.round(F.percent_rank().over(w_source_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.round(F.cume_dist().over(w_source_by_played_at), 3).alias("played_at_cume_dist"),
        F.lag("ms_played").over(w_source_by_played_at).alias("prev_ms_played"),
        F.sum("ms_played").over(w_played_at_month).alias("played_at_month_sum_ms_played"),
    )
)

derived = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "ms_played_tier",
        "played_at_month_avg_ms_played",
        "next_played_at",
        "distinct_ms_played_tier_in_played_at_month",
        "rows_in_played_at_month",
        "second_played_at",
        "played_at_percent_rank",
        "played_at_cume_dist",
        "prev_ms_played",
        "played_at_month_sum_ms_played",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

sequenced_2 = (
    derived
    .select(
        "*",
        F.round(F.col("prev_ms_played") / F.nullif(F.sum("prev_ms_played").over(w_distinct_ms_played_tier_in_p), F.lit(0)), 4).alias("prev_ms_played_share_of_distinct_ms_played_tier_"),
        F.size(F.collect_set("device").over(w_distinct_ms_played_tier_in_p)).alias("distinct_device_in_distinct_ms_played_tier_in_pl"),
        F.first("next_played_at").over(w_all_by_next_played_at_full).alias("first_next_played_at"),
        F.count(F.lit(1)).over(w_second_played_at_month_by_se).alias("running_count"),
        F.lead("rows_in_played_at_month").over(w_distinct_ms_played_tier_in_p_2).alias("next_rows_in_played_at_month"),
    )
)

result = (
    sequenced_2
    .filter(F.col("distinct_ms_played_tier_in_played_at_month") <= 8)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played_tier",
        "next_played_at",
        "rows_in_played_at_month",
        "played_at_percent_rank",
        "played_at_cume_dist",
        "prev_ms_played",
        "played_at_month_sum_ms_played",
        "ms_played_filled",
        "first_next_played_at",
        "running_count",
        "next_rows_in_played_at_month",
        (F.col("next_played_at") + F.expr("INTERVAL 15 MINUTES")).alias("next_played_at_plus_15m"),
    )
    .orderBy("listener_id")
)
