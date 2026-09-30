from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at_moving = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(-6, Window.currentRow)
w_track_id_source = Window.partitionBy("track_id", "source")
w_track_id = Window.partitionBy("track_id")
w_device_track_id_by_played_at = Window.partitionBy("device", "track_id").orderBy("played_at", "id")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_listener_id_device = Window.partitionBy("listener_id", "device")
w_listener_plan_source_by_play = Window.partitionBy("listener_plan", "source").orderBy("played_at")
w_track_id_by_played_at = Window.partitionBy("track_id").orderBy("played_at", "id")
w_device_by_played_at = Window.partitionBy("device").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_listener_plan_source_by_play_2 = Window.partitionBy("listener_plan", "source").orderBy("played_at", "id").rowsBetween(-6, -1)
w_prev2_played_at_year_median = Window.partitionBy("prev2_played_at_year", "median_played_at_dense_rank").orderBy("distinct_ids", "played_at_month", "played_at_dense_rank", "prev2_played_at_year")
w_n_rows_distinct_track_ids = Window.partitionBy("n_rows", "distinct_track_ids")

valid_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
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

combined_details = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select("s.*", F.col("l.plan").alias("listener_plan"))
)

windowed = (
    combined_details
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_track_id_by_played_at_moving), 2).alias("ms_played_avg_7"),
        F.min("ms_played").over(w_track_id_source).alias("track_id_source_min_ms_played"),
        F.max("ms_played").over(w_track_id).alias("track_id_max_ms_played"),
        F.lag("played_at", 2).over(w_device_track_id_by_played_at).alias("prev2_played_at"),
        F.ntile(3).over(w_all_by_played_at).alias("played_at_tercile"),
        F.size(F.collect_set("listener_plan").over(w_track_id)).alias("distinct_listener_plan_in_track_id"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id_source), F.lit(0)), 4).alias("ms_played_share_of_track_id_source"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id_device), F.lit(0)), 4).alias("ms_played_share_of_listener_id_device"),
        F.size(F.collect_set("source").over(w_track_id)).alias("distinct_source_in_track_id"),
        F.size(F.collect_set("track_id").over(w_listener_id_device)).alias("distinct_track_id_in_listener_id_device"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.dense_rank().over(w_listener_plan_source_by_play).alias("played_at_dense_rank"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_track_id_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.lead("played_at").over(w_device_by_played_at).alias("next_played_at"),
        F.max("ms_played").over(w_listener_plan_source_by_play_2).alias("ms_played_max_6_prior"),
    )
)

rollup_base = (
    windowed
    .filter(F.col("ms_played_share_of_track_id_source") < 0.6)
    .groupBy(
        F.date_trunc("month", "played_at").alias("played_at_month"),
        "played_at_dense_rank",
        F.year("prev2_played_at").alias("prev2_played_at_year"),
    )
    .agg(
        F.percentile_approx("played_at_dense_rank", 0.5).alias("median_played_at_dense_rank"),
        F.array_sort(F.collect_set("listener_plan")).alias("listener_plan_list"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

sequenced = (
    rollup_base
    .select(
        "*",
        (F.col("distinct_ids") - F.lag("distinct_ids").over(w_prev2_played_at_year_median)).alias("distinct_ids_change"),
        F.round(F.col("median_played_at_dense_rank") - F.avg("median_played_at_dense_rank").over(w_n_rows_distinct_track_ids), 2).alias("median_played_at_dense_rank_vs_n_rows_distinct_t"),
    )
)

result = (
    sequenced
    .select(
        "played_at_month",
        "prev2_played_at_year",
        "median_played_at_dense_rank",
        "n_rows",
        "device_list",
        "distinct_track_ids",
        "distinct_ids",
        "median_played_at_dense_rank_vs_n_rows_distinct_t",
    )
    .orderBy(F.col("played_at_month").asc_nulls_last(), "prev2_played_at_year")
)
