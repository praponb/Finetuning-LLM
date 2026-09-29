from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_ms_played_running = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")
w_device_by_ms_played_ties = Window.partitionBy("device").orderBy("ms_played")
w_device_distinct_source_in_li = Window.partitionBy("device", "distinct_source_in_listener_id")
w_distinct_source_in_listener = Window.partitionBy("distinct_source_in_listener_id").orderBy("listener_id_min_ms_played", "id")
w_distinct_source_in_listener_2 = Window.partitionBy("distinct_source_in_listener_id").orderBy("listener_id_min_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("device").orderBy(F.col("device_distinct_source_in_listener_id_avg_ms_pla").desc(), F.col("id").desc())

recent_plays = (
    df_plays
    .filter((~F.col("source").isin("playlist", "album")) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

metrics = (
    recent_plays
    .select(
        "id",
        "played_at",
        "ms_played",
        "device",
        F.count(F.lit(1)).over(w_device_by_ms_played_running).alias("running_count"),
        F.min("ms_played").over(w_listener_id).alias("listener_id_min_ms_played"),
        F.round(F.col("ms_played_filled") - F.avg("ms_played_filled").over(w_listener_id), 2).alias("ms_played_filled_vs_listener_id_avg"),
        F.size(F.collect_set("source").over(w_listener_id)).alias("distinct_source_in_listener_id"),
        F.dense_rank().over(w_device_by_ms_played_ties).alias("ms_played_dense_rank"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "ms_played",
        "device",
        "listener_id_min_ms_played",
        "ms_played_filled_vs_listener_id_avg",
        "ms_played_dense_rank",
        F.max("running_count").over(w_device_distinct_source_in_li).alias("device_distinct_source_in_listener_id_max_runnin"),
        F.lag("running_count").over(w_distinct_source_in_listener).alias("prev_running_count"),
        F.sum("running_count").over(w_distinct_source_in_listener_2).alias("running_running_count"),
        F.round(F.avg("ms_played_dense_rank").over(w_device_distinct_source_in_li), 2).alias("device_distinct_source_in_listener_id_avg_ms_pla"),
        F.count(F.lit(1)).over(w_device_distinct_source_in_li).alias("rows_in_device_distinct_source_in_listener_id"),
    )
)

with_activity = (
    windowed
    .crossJoin(windowed.agg(F.avg("ms_played_dense_rank").alias("overall_avg_ms_played_dense_rank")))
    .filter(F.col("ms_played_dense_rank") > F.col("overall_avg_ms_played_dense_rank"))
    .drop("overall_avg_ms_played_dense_rank")
)

unique_rows = (
    with_activity
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

device_stats = (
    unique_rows
    .groupBy("device")
    .agg(F.avg("running_running_count").alias("avg_running_running_count"))
)

vs_group = (
    unique_rows.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.running_running_count") > F.col("g.avg_running_running_count"))
    .select(
        "s.*",
        "g.avg_running_running_count",
        F.round(F.col("s.running_running_count") - F.col("g.avg_running_running_count"), 2).alias("running_running_count_minus_avg"),
    )
)

result = (
    vs_group
    .groupingSets(
        [["device", "ms_played_dense_rank"], ["device"]],
        "device", "ms_played_dense_rank",
    )
    .agg(F.max("running_running_count").alias("max_running_running_count"))
    .orderBy(
        F.col("device").asc_nulls_last(),
        F.col("ms_played_dense_rank").asc_nulls_last(),
    )
)
