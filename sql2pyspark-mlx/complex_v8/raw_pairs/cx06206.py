from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month = Window.partitionBy(F.date_trunc("month", "played_at"))
w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played_filled").desc(), "id")
w_device_listener_id_by_ms_pla_2 = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_track_id_by_ms_played = Window.partitionBy("source", "track_id").orderBy("ms_played_filled", "id")

scoped_plays = (
    df_plays
    .filter((F.to_date(F.col("played_at")) < F.lit("2025-02-06").cast("date")) & (F.col("track_id") >= 26))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

annotated = (
    scoped_plays
    .withColumn("device_source_label_ref", F.concat(F.col("device_source_label"), F.lit("#"), F.col("ms_played_filled").cast("string")))
)

windowed = (
    annotated
    .select(
        "id",
        "played_at",
        "device",
        "source",
        "source_prefix",
        "device_source_label",
        "device_source_label_ref",
        F.round(F.avg("ms_played_filled").over(w_played_at_month), 2).alias("played_at_month_avg_ms_played_filled"),
        F.round(F.col("ms_played_filled") - F.avg("ms_played_filled").over(w_played_at_month), 2).alias("ms_played_filled_vs_played_at_month_avg"),
        F.rank().over(w_device_listener_id_by_ms_pla).alias("ms_played_filled_rank"),
        F.first("ms_played_filled").over(w_device_listener_id_by_ms_pla_2).alias("first_ms_played_filled"),
        F.round(F.cume_dist().over(w_source_track_id_by_ms_played), 3).alias("ms_played_filled_cume_dist"),
    )
)

result = (
    windowed
    .groupBy("source")
    .pivot("device", ["tv", "car", "speaker", "desktop"])
    .agg(F.min("played_at_month_avg_ms_played_filled").alias("min_v"), F.sum("played_at_month_avg_ms_played_filled").alias("sum_v"))
)
