from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_played_at = Window.partitionBy("device").orderBy("played_at", "id")
w_all_by_ms_played_filled_2_ru = Window.orderBy(F.col("ms_played_filled_2").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_last_known_playe = Window.partitionBy("track_id").orderBy("last_known_played_at", "id")
w_track_id_by_last_known_playe_2 = Window.partitionBy("track_id").orderBy("last_known_played_at", "id").rowsBetween(-2, Window.currentRow)

valid_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled_2"),
    )
)

with_trends = (
    valid_plays
    .withColumn("played_at_dense_rank", F.dense_rank().over(w_device_by_played_at))
    .withColumn("last_known_played_at", F.last("played_at", ignorenulls=True).over(w_all_by_ms_played_filled_2_ru))
)

with_trends_2 = (
    with_trends
    .withColumn("last_known_played_at_percent_rank", F.round(F.percent_rank().over(w_track_id_by_last_known_playe), 3))
    .withColumn("played_at_dense_rank_avg_3", F.round(F.avg("played_at_dense_rank").over(w_track_id_by_last_known_playe_2), 2))
)

result = (
    with_trends_2
    .groupBy("listener_id")
    .pivot("source", ["radio", "playlist", "search"])
    .agg(F.min("played_at_dense_rank"))
    .orderBy("listener_id")
)
