from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_played_at_hou = Window.partitionBy("listener_id").orderBy("played_at_hour")
w_device_group_by_played_at_ho = Window.partitionBy("device_group").orderBy("played_at_hour")
w_played_at_hour_source = Window.partitionBy("played_at_hour", "source")
w_listener_id_by_played_at_hou_2 = Window.partitionBy("listener_id").orderBy("played_at_hour", "id")
w_all_by_played_at_hour_full = Window.orderBy(F.col("played_at_hour").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_hour_dense = Window.orderBy(F.col("played_at_hour_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id = Window.partitionBy("listener_id")
w_all_by_rn = Window.orderBy("rn", "id")

base = (
    df_plays
    .filter(F.col("device") == "car")
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.when(F.col("device").isin("speaker", "car"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        F.when(F.col("ms_played") >= 220498, "HIGH").when(F.col("ms_played") >= 157900, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        F.hour(F.col("played_at")).alias("played_at_hour"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.dense_rank().over(w_listener_id_by_played_at_hou).alias("played_at_hour_dense_rank"),
        F.round(F.percent_rank().over(w_device_group_by_played_at_ho), 3).alias("played_at_hour_percent_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_hour_source), 2).alias("ms_played_vs_played_at_hour_source_avg"),
        F.row_number().over(w_listener_id_by_played_at_hou_2).alias("rn"),
        F.last("played_at_hour").over(w_all_by_played_at_hour_full).alias("last_played_at_hour"),
        F.first("played_at_hour").over(w_all_by_played_at_hour_full).alias("first_played_at_hour"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.last("rn").over(w_all_by_played_at_hour_dense).alias("last_rn"),
        F.round(F.avg("played_at_hour_dense_rank").over(w_listener_id), 2).alias("listener_id_avg_played_at_hour_dense_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id), 2).alias("ms_played_vs_listener_id_avg"),
        F.ntile(5).over(w_all_by_rn).alias("rn_quintile"),
    )
)

result = (
    metrics
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "device_group",
        "ms_played_tier",
        "played_at_hour",
        "played_at_hour_dense_rank",
        "played_at_hour_percent_rank",
        "ms_played_vs_played_at_hour_source_avg",
        "rn",
        "last_played_at_hour",
        "first_played_at_hour",
        "last_rn",
        "listener_id_avg_played_at_hour_dense_rank",
        "ms_played_vs_listener_id_avg",
        F.when(F.col("first_played_at_hour") >= 13, "large").when(F.col("first_played_at_hour") >= 9, "medium").otherwise("small").alias("first_played_at_hour_tier"),
        F.round(F.col("played_at_hour") / F.nullif(F.col("first_played_at_hour"), F.lit(0)), 3).alias("played_at_hour_per_first_played_at_hour"),
    )
    .orderBy("last_rn", F.col("listener_id_avg_played_at_hour_dense_rank").desc())
)
