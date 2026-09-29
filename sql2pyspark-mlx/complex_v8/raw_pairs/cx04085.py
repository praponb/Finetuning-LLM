from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_source_device_by_ms_played_d = Window.partitionBy("source", "device").orderBy("ms_played_distance")
w_source_listener_id_by_ms_pla = Window.partitionBy("source", "listener_id").orderBy("ms_played", "id")
w_source_by_ms_played = Window.partitionBy("source").orderBy("ms_played", "id")
w_source_device_by_ms_played_d_2 = Window.partitionBy("source", "device").orderBy("ms_played_distance", "id").rowsBetween(-3, -1)
w_source_device_by_ms_played_d_3 = Window.partitionBy("source", "device").orderBy("ms_played_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_plays = (
    df_plays
    .filter(~F.col("source").isin("radio", "album"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 151112).alias("ms_played_distance"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

windowed = (
    filtered_plays
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "source",
        "played_at_label",
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.dense_rank().over(w_source_device_by_ms_played_d).alias("ms_played_distance_dense_rank"),
        F.round(F.percent_rank().over(w_source_listener_id_by_ms_pla), 3).alias("ms_played_percent_rank"),
        (F.col("ms_played") - F.lag("ms_played").over(w_source_by_ms_played)).alias("ms_played_change"),
        F.round(F.avg("ms_played").over(w_source_device_by_ms_played_d_2), 2).alias("ms_played_avg_3_prior"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id), 2).alias("ms_played_vs_listener_id_avg"),
        F.max("ms_played").over(w_listener_id).alias("listener_id_max_ms_played"),
        F.count(F.lit(1)).over(w_source_device_by_ms_played_d_3).alias("running_count"),
    )
)

top_rows = windowed.filter((F.col("ms_played_distance_dense_rank") <= 5) & (~F.col("is_skipped")))

per_group = (
    top_rows
    .groupBy(
        "listener_id",
        F.when(F.col("ms_played_change") >= 160000, "A").when(F.col("ms_played_change") >= -160000, "B").otherwise("C").alias("ms_played_change_tier"),
        "ms_played_distance_dense_rank",
    )
    .agg(
        F.round(F.stddev("listener_id_max_ms_played"), 2).alias("stddev_listener_id_max_ms_played"),
        F.round(F.sum("listener_id_max_ms_played") / F.nullif(F.sum("ms_played_vs_listener_id_avg"), F.lit(0)), 3).alias("listener_id_max_ms_played_to_ms_played_vs_listen"),
        F.round(F.stddev("ms_played_percent_rank"), 2).alias("stddev_ms_played_percent_rank"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.sum("running_count").alias("total_running_count"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
    )
)

result = (
    per_group
    .select(
        "listener_id",
        "ms_played_change_tier",
        "ms_played_distance_dense_rank",
        "listener_id_max_ms_played_to_ms_played_vs_listen",
        "stddev_ms_played_percent_rank",
        "device_list",
        "source_list",
        F.lower(F.regexp_replace(F.trim(F.col("ms_played_change_tier")), " ", "_")).alias("ms_played_change_tier_slug"),
    )
    .orderBy(
        "ms_played_change_tier_slug",
        F.col("listener_id").desc_nulls_first(),
        "ms_played_change_tier",
        "ms_played_distance_dense_rank",
    )
    .limit(20)
)
