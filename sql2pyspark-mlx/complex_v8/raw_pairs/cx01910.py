from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_prior_6 = Window.orderBy("played_at", "id").rowsBetween(-6, -1)
w_device_track_id = Window.partitionBy("device", "track_id")

cleaned = (
    df_plays
    .filter((F.col("track_id") >= 25) & (F.col("played_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
        F.concat(F.col("device"), F.lit("#"), F.col("id").cast("string")).alias("device_ref"),
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_all_by_played_at_prior_6), 2).alias("ms_played_avg_6_prior"),
        F.round(F.avg("ms_played").over(w_device_track_id), 2).alias("device_track_id_avg_ms_played"),
        F.sum("ms_played").over(w_device_track_id).alias("device_track_id_sum_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device_track_id), 2).alias("ms_played_vs_device_track_id_avg"),
    )
)

result = (
    windowed
    .groupBy("played_at_month", "listener_id")
    .pivot("source", ["search", "radio", "album", "playlist"])
    .agg(F.sum("ms_played_vs_device_track_id_avg"))
    .orderBy("played_at_month", "listener_id")
)
