from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_moving_3 = Window.orderBy("played_at", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 308104, "large").when(F.col("ms_played") >= 107912, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

ranked = (
    base
    .select(
        "*",
        F.size(F.collect_set("listener_id").over(w_device)).alias("distinct_listener_id_in_device"),
        F.last("ms_played").over(w_all_by_played_at_full).alias("last_ms_played"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.sum("ms_played").over(w_all_by_played_at_moving_3).alias("ms_played_sum_4"),
    )
)

with_activity = (
    ranked.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") >= 7), "left_semi")
    .filter(F.col("is_skipped"))
)

result = (
    with_activity
    .groupBy("listener_id")
    .pivot("source", ["playlist", "radio", "album", "search"])
    .agg(F.sum("distinct_listener_id_in_device").alias("amount"), F.count("distinct_listener_id_in_device").alias("n"))
    .orderBy("listener_id")
)
