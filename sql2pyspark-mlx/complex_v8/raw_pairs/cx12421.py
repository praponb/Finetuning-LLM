from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_plan_is_skipped = Window.partitionBy("listener_plan", "is_skipped")
w_device_listener_plan_by_ms_p = Window.partitionBy("device", "listener_plan").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, Window.currentRow)
w_is_skipped_listener_plan_by = Window.partitionBy("is_skipped", "listener_plan").orderBy("ms_played", "id")
w_device_listener_plan_by_ms_p_2 = Window.partitionBy("device", "listener_plan").orderBy(F.col("ms_played").desc(), "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_device_by_ms_played_running = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_ties = Window.orderBy(F.col("ms_played").desc())
w_device_by_ms_played = Window.partitionBy("device").orderBy("ms_played", "id")
w_device_listener_plan_by_ms_p_3 = Window.partitionBy("device", "listener_plan").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_by_ms_played_full = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_listener_plan_by_ms_p_4 = Window.partitionBy("device", "listener_plan").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, -1)
w_ms_played_rank_track_id_by_m = Window.partitionBy("ms_played_rank", "track_id").orderBy("ms_played_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_track_id_in_listene = Window.partitionBy("distinct_track_id_in_listener_plan_is_skipped", "listener_id")
w_ms_played_rank_track_id_by_m_2 = Window.partitionBy("ms_played_rank", "track_id").orderBy("ms_played_rank", "id")
w_ms_played_rank_track_id_by_m_3 = Window.partitionBy("ms_played_rank", "track_id").orderBy("ms_played_rank", "id").rowsBetween(-3, Window.currentRow)
w_distinct_track_id_in_listene_2 = Window.partitionBy("distinct_track_id_in_listener_plan_is_skipped", "ms_played_tercile").orderBy(F.col("ms_played_avg_2").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_sum_6_prior = Window.orderBy("ms_played_sum_6_prior")

valid = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(8, 13))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

enriched_listener = (
    valid.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        F.col("l.plan").alias("listener_plan"),
    )
)

with_trends = (
    enriched_listener
    .select(
        "*",
        F.size(F.collect_set("track_id").over(w_listener_plan_is_skipped)).alias("distinct_track_id_in_listener_plan_is_skipped"),
        F.count(F.lit(1)).over(w_listener_plan_is_skipped).alias("rows_in_listener_plan_is_skipped"),
        F.round(F.avg("ms_played").over(w_device_listener_plan_by_ms_p), 2).alias("ms_played_avg_2"),
        F.lead("ms_played", 2).over(w_is_skipped_listener_plan_by).alias("next2_ms_played"),
        F.rank().over(w_device_listener_plan_by_ms_p_2).alias("ms_played_rank"),
        F.ntile(3).over(w_is_skipped_listener_plan_by).alias("ms_played_tercile"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped), 2).alias("ms_played_vs_is_skipped_avg"),
        F.sum("ms_played").over(w_device_by_ms_played_running).alias("running_ms_played"),
        F.round(F.avg("ms_played").over(w_listener_plan_is_skipped), 2).alias("listener_plan_is_skipped_avg_ms_played"),
        F.rank().over(w_all_by_ms_played_ties).alias("ms_played_rank_2"),
        F.ntile(5).over(w_device_by_ms_played).alias("ms_played_quintile"),
        F.sum("ms_played").over(w_device_listener_plan_by_ms_p_3).alias("running_ms_played_2"),
        F.last("ms_played").over(w_device_by_ms_played_full).alias("last_ms_played"),
        F.sum("ms_played").over(w_device_listener_plan_by_ms_p_4).alias("ms_played_sum_6_prior"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_plan_is_skipped), 2).alias("ms_played_vs_listener_plan_is_skipped_avg"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.count(F.lit(1)).over(w_ms_played_rank_track_id_by_m).alias("running_count"),
        F.min("ms_played_rank").over(w_distinct_track_id_in_listene).alias("distinct_track_id_in_listener_plan_is_skipped_li"),
        F.count(F.lit(1)).over(w_distinct_track_id_in_listene).alias("rows_in_distinct_track_id_in_listener_plan_is_sk"),
        F.min("ms_played_avg_2").over(w_distinct_track_id_in_listene).alias("distinct_track_id_in_listener_plan_is_skipped_li_2"),
        F.lead("ms_played_rank", 2, 0).over(w_ms_played_rank_track_id_by_m_2).alias("next2_ms_played_rank"),
        F.sum("running_ms_played").over(w_ms_played_rank_track_id_by_m_3).alias("running_ms_played_sum_4"),
        F.last("ms_played_tercile").over(w_distinct_track_id_in_listene_2).alias("last_ms_played_tercile"),
        F.round(F.cume_dist().over(w_all_by_ms_played_sum_6_prior), 3).alias("ms_played_sum_6_prior_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "listener_plan",
        "distinct_track_id_in_listener_plan_is_skipped",
        "ms_played_avg_2",
        "ms_played_rank",
        "ms_played_tercile",
        "ms_played_vs_is_skipped_avg",
        "listener_plan_is_skipped_avg_ms_played",
        "ms_played_quintile",
        "running_ms_played_2",
        "ms_played_vs_listener_plan_is_skipped_avg",
        "running_count",
        "distinct_track_id_in_listener_plan_is_skipped_li",
        "rows_in_distinct_track_id_in_listener_plan_is_sk",
        "next2_ms_played_rank",
        "last_ms_played_tercile",
        "ms_played_sum_6_prior_cume_dist",
        F.upper(F.trim(F.col("listener_plan"))).alias("listener_plan_clean"),
        F.when(F.col("next2_ms_played_rank") >= 7, "large").when(F.col("next2_ms_played_rank") >= 4, "medium").otherwise("small").alias("next2_ms_played_rank_tier"),
    )
    .orderBy("listener_plan", F.col("ms_played_sum_6_prior_cume_dist").asc())
)
