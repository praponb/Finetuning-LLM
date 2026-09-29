from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_device = Window.partitionBy("listener_id", "device")
w_all_by_played_at_plus_15m = Window.orderBy(F.col("played_at_plus_15m").desc(), "id")
w_track_id = Window.partitionBy("track_id")
w_device_group_by_played_at_pl = Window.partitionBy("device_group").orderBy("played_at_plus_15m", "id")
w_listener_id_device_group_by = Window.partitionBy("listener_id", "device_group").orderBy("played_at_plus_15m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_device_group_by_2 = Window.partitionBy("listener_id", "device_group").orderBy("played_at_plus_15m", "id").rowsBetween(-1, Window.currentRow)
w_all_by_played_at_plus_15m_ti = Window.orderBy(F.col("played_at_plus_15m").desc())
w_listener_id_device_group_by_3 = Window.partitionBy("listener_id", "device_group").orderBy("played_at_plus_15m", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_plus_15m_fu = Window.orderBy(F.col("played_at_plus_15m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("ms_played") < 223262))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 15 MINUTES")).alias("played_at_plus_15m"),
        F.when(F.col("device").isin("speaker", "phone"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id_device), 2).alias("ms_played_vs_listener_id_device_avg"),
        F.rank().over(w_all_by_played_at_plus_15m).alias("played_at_plus_15m_rank"),
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id"),
        F.max("ms_played").over(w_listener_id_device).alias("listener_id_device_max_ms_played"),
        F.ntile(3).over(w_device_group_by_played_at_pl).alias("played_at_plus_15m_tercile"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id), 2).alias("ms_played_vs_track_id_avg"),
        F.sum("ms_played").over(w_listener_id_device).alias("listener_id_device_sum_ms_played"),
        F.count(F.lit(1)).over(w_listener_id_device).alias("rows_in_listener_id_device"),
        F.first("ms_played").over(w_listener_id_device_group_by).alias("first_ms_played"),
        F.max("ms_played").over(w_track_id).alias("track_id_max_ms_played"),
        F.round(F.avg("ms_played").over(w_listener_id_device_group_by_2), 2).alias("ms_played_avg_2"),
        F.dense_rank().over(w_all_by_played_at_plus_15m_ti).alias("played_at_plus_15m_dense_rank"),
        F.min("ms_played").over(w_listener_id_device_group_by_3).alias("min_ms_played_so_far"),
        F.nth_value("ms_played", 2).over(w_all_by_played_at_plus_15m_fu).alias("second_ms_played"),
    )
)

result = (
    metrics
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "device_group",
        "ms_played_vs_listener_id_device_avg",
        "played_at_plus_15m_rank",
        "distinct_listener_id_in_track_id",
        "listener_id_device_max_ms_played",
        "played_at_plus_15m_tercile",
        "ms_played_vs_track_id_avg",
        "listener_id_device_sum_ms_played",
        "rows_in_listener_id_device",
        "first_ms_played",
        "track_id_max_ms_played",
        "ms_played_avg_2",
        "played_at_plus_15m_dense_rank",
        "min_ms_played_so_far",
        "second_ms_played",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        F.concat_ws(": ", F.col("device_group"), F.col("device")).alias("device_group_device_label"),
    )
)
