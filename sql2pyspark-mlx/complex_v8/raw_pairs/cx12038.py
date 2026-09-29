from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_played_at_month = Window.partitionBy("listener_id", F.date_trunc("month", "played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id = Window.partitionBy("track_id")
w_listener_id_played_at_month_2 = Window.partitionBy("listener_id", F.date_trunc("month", "played_at")).orderBy("played_at")
w_device = Window.partitionBy("device")
w_played_at_year_listener_id_b = Window.partitionBy(F.year("played_at"), "listener_id").orderBy("ms_played", "id")
w_listener_id_played_at_month_3 = Window.partitionBy("listener_id", F.date_trunc("month", "played_at")).orderBy("played_at", "id").rowsBetween(-2, -1)

valid = (
    df_plays
    .filter(F.col("source").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.max("ms_played").over(w_listener_id_played_at_month).alias("max_ms_played_so_far"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.min("ms_played").over(w_track_id).alias("track_id_min_ms_played"),
        F.round(F.cume_dist().over(w_listener_id_played_at_month_2), 3).alias("played_at_cume_dist"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device), 2).alias("ms_played_vs_device_avg"),
        F.row_number().over(w_played_at_year_listener_id_b).alias("seq_no"),
        F.round(F.avg("ms_played").over(w_listener_id_played_at_month_3), 2).alias("ms_played_avg_2_prior"),
        F.sum("ms_played").over(w_track_id).alias("track_id_sum_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device), F.lit(0)), 4).alias("ms_played_share_of_device"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.round(F.avg("rows_in_track_id").over(w_device), 2).alias("device_avg_rows_in_track_id"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.sum("ms_played_share_of_device").over(w_device).alias("device_sum_ms_played_share_of_device"),
        F.max("rows_in_track_id").over(w_device).alias("device_max_rows_in_track_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "device_clean",
        "max_ms_played_so_far",
        "rows_in_track_id",
        "track_id_min_ms_played",
        "ms_played_vs_device_avg",
        "seq_no",
        "ms_played_avg_2_prior",
        "track_id_sum_ms_played",
        "ms_played_share_of_device",
        "device_avg_rows_in_track_id",
        "device_sum_ms_played_share_of_device",
        "device_max_rows_in_track_id",
        F.hour(F.col("played_at")).alias("played_at_hour"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)
