from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_listener_id = Window.partitionBy("listener_id")
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_listener_id_by_ms_played_pri = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(-2, -1)
w_listener_plan_track_id_by_ms = Window.partitionBy("listener_plan", "track_id").orderBy(F.col("ms_played").desc(), "id")
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id")
w_all_by_listener_id_sum_ms_pl = Window.orderBy("listener_id_sum_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_plan_by_distinct_so = Window.partitionBy("listener_plan").orderBy(F.col("distinct_source_in_track_id_listener_id").asc_nulls_last(), "id")

scoped_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

combined_details = (
    scoped_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device), F.lit(0)), 4).alias("ms_played_share_of_device"),
        F.round(F.avg("ms_played").over(w_listener_id), 2).alias("listener_id_avg_ms_played"),
        F.size(F.collect_set("listener_plan").over(w_listener_id)).alias("distinct_listener_plan_in_listener_id"),
        F.size(F.collect_set("source").over(w_track_id_listener_id)).alias("distinct_source_in_track_id_listener_id"),
        F.round(F.avg("ms_played").over(w_listener_id_by_ms_played_pri), 2).alias("ms_played_avg_2_prior"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id_listener_id), F.lit(0)), 4).alias("ms_played_share_of_track_id_listener_id"),
        F.lag("ms_played", 2).over(w_listener_plan_track_id_by_ms).alias("prev2_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_listener_id), 2).alias("ms_played_vs_track_id_listener_id_avg"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
        F.lag("ms_played").over(w_listener_id_by_ms_played).alias("prev_ms_played"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.sum("ms_played_avg_2_prior").over(w_all_by_listener_id_sum_ms_pl).alias("running_ms_played_avg_2_prior"),
        F.min("prev_ms_played").over(w_device).alias("device_min_prev_ms_played"),
        F.lead("listener_id_sum_ms_played", 2).over(w_listener_plan_by_distinct_so).alias("next2_listener_id_sum_ms_played"),
        F.max("ms_played").over(w_device).alias("device_max_ms_played"),
        F.min("ms_played_share_of_device").over(w_device).alias("device_min_ms_played_share_of_device"),
    )
)

stats = (
    metrics
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("ms_played_share_of_device").alias("total_ms_played_share_of_device"),
    )
)

result = (
    stats
    .filter(F.col("listener_id").isNotNull())
    .select(
        "listener_id",
        "row_count",
        "total_ms_played_share_of_device",
        F.when(F.col("row_count") >= 9, "HIGH").when(F.col("row_count") >= 5, "MEDIUM").otherwise("LOW").alias("row_count_tier"),
    )
    .orderBy("total_ms_played_share_of_device", "listener_id")
    .limit(10)
)
