from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_all_by_played_at_moving_2 = Window.orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_ms_played_2 = Window.orderBy(F.col("ms_played").desc(), "id")
w_all_by_ms_played_ties = Window.orderBy("ms_played")
w_all_by_ms_played_full = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_prior_2 = Window.orderBy("played_at", "id").rowsBetween(-2, -1)
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_prior_3 = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(-3, -1)
w_all_by_ms_played_moving_1 = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, Window.currentRow)
w_ms_played_quintile = Window.partitionBy("ms_played_quintile")
w_all_by_rows_in_listener_id_m = Window.orderBy("rows_in_listener_id", "id").rowsBetween(-2, Window.currentRow)
w_is_skipped_by_ms_played_avg = Window.partitionBy("is_skipped").orderBy("ms_played_avg_2", "id").rowsBetween(-2, -1)
w_all_by_rows_in_listener_id_r = Window.orderBy("rows_in_listener_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_rows_in_listener_id_m_2 = Window.orderBy("rows_in_listener_id", "id").rowsBetween(-6, Window.currentRow)
w_all_by_rows_in_listener_id = Window.orderBy("rows_in_listener_id", "id")

filtered_plays = (
    df_plays
    .filter((F.col("ms_played") >= 99376) & (F.col("listener_id").isNotNull()))
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

sequenced = (
    filtered_plays
    .select(
        "*",
        F.max("ms_played").over(w_listener_id).alias("listener_id_max_ms_played"),
        F.ntile(3).over(w_all_by_ms_played).alias("ms_played_tercile"),
        F.sum("ms_played").over(w_all_by_played_at_moving_2).alias("ms_played_sum_3"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.dense_rank().over(w_all_by_ms_played).alias("ms_played_dense_rank"),
        F.ntile(5).over(w_all_by_ms_played).alias("ms_played_quintile"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_ms_played_2))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.round(F.percent_rank().over(w_all_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
        F.nth_value("played_at", 2).over(w_all_by_ms_played_full).alias("second_played_at"),
        F.round(F.avg("ms_played").over(w_all_by_played_at_prior_2), 2).alias("ms_played_avg_2_prior"),
        F.first("played_at", ignorenulls=True).over(w_all_by_ms_played_running).alias("first_known_played_at"),
        F.round(F.percent_rank().over(w_all_by_ms_played_2), 3).alias("ms_played_percent_rank_2"),
        F.sum("ms_played").over(w_all_by_ms_played_prior_3).alias("ms_played_sum_3_prior"),
        F.round(F.avg("ms_played").over(w_all_by_ms_played_moving_1), 2).alias("ms_played_avg_2"),
    )
)

enriched = (
    sequenced
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "listener_id_max_ms_played",
        "ms_played_tercile",
        "ms_played_sum_3",
        "rows_in_listener_id",
        "ms_played_quintile",
        "ms_played_percent_rank",
        "ms_played_avg_2_prior",
        "ms_played_percent_rank_2",
        "ms_played_sum_3_prior",
        "ms_played_avg_2",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "listener_id_max_ms_played",
        "ms_played_sum_3",
        "rows_in_listener_id",
        "ms_played_avg_2_prior",
        "ms_played_percent_rank_2",
        "ms_played_sum_3_prior",
        "ms_played_avg_2",
        F.size(F.collect_set("track_id").over(w_ms_played_quintile)).alias("distinct_track_id_in_ms_played_quintile"),
        F.sum("rows_in_listener_id").over(w_all_by_rows_in_listener_id_m).alias("rows_in_listener_id_sum_3"),
        F.sum("ms_played_quintile").over(w_is_skipped_by_ms_played_avg).alias("ms_played_quintile_sum_2_prior"),
        F.first("rows_in_listener_id", ignorenulls=True).over(w_all_by_rows_in_listener_id_r).alias("first_known_rows_in_listener_id"),
        F.max("ms_played").over(w_all_by_rows_in_listener_id_m_2).alias("ms_played_max_7"),
        F.ntile(3).over(w_all_by_rows_in_listener_id).alias("rows_in_listener_id_tercile"),
        F.max("listener_id_max_ms_played").over(w_ms_played_quintile).alias("ms_played_quintile_max_listener_id_max_ms_played"),
        F.rank().over(w_all_by_rows_in_listener_id).alias("rows_in_listener_id_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "rows_in_listener_id",
        "ms_played_avg_2",
        "distinct_track_id_in_ms_played_quintile",
        "rows_in_listener_id_sum_3",
        "ms_played_quintile_sum_2_prior",
        "first_known_rows_in_listener_id",
        "rows_in_listener_id_tercile",
        "ms_played_quintile_max_listener_id_max_ms_played",
        "rows_in_listener_id_rank",
        F.abs(F.col("ms_played_percent_rank_2") - 0.7).alias("ms_played_percent_rank_2_distance"),
    )
    .orderBy(
        F.col("first_known_rows_in_listener_id").desc_nulls_first(),
        F.col("ms_played_percent_rank_2_distance").desc(),
        "id",
    )
    .limit(20)
)
