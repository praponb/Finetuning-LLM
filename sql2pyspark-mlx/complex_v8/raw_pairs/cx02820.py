from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_device_by_played_at = Window.partitionBy("track_id", "device").orderBy(F.col("played_at").desc())
w_listener_id_track_id_by_play = Window.partitionBy("listener_id", "track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_listener_id = Window.partitionBy("device", "listener_id")
w_track_id_device_by_played_at_2 = Window.partitionBy("track_id", "device").orderBy(F.col("played_at").desc(), "id")
w_all_by_days_since_played_at = Window.orderBy("days_since_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_device_by_device = Window.partitionBy("is_skipped", "device").orderBy("device_listener_id_max_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

with_windows = (
    base_plays
    .select(
        "*",
        F.round(F.percent_rank().over(w_track_id_device_by_played_at), 3).alias("played_at_percent_rank"),
        F.last("days_since_played_at", ignorenulls=True).over(w_listener_id_track_id_by_play).alias("last_known_days_since_played_at"),
        F.max("ms_played").over(w_device_listener_id).alias("device_listener_id_max_ms_played"),
        F.dense_rank().over(w_track_id_device_by_played_at_2).alias("played_at_dense_rank"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "ms_played_filled",
        "days_since_played_at",
        "last_known_days_since_played_at",
        "device_listener_id_max_ms_played",
        "played_at_dense_rank",
        F.min("played_at_dense_rank").over(w_all_by_days_since_played_at).alias("min_played_at_dense_rank_so_far"),
        F.first("played_at_percent_rank", ignorenulls=True).over(w_is_skipped_device_by_device).alias("first_known_played_at_percent_rank"),
    )
)

qualified = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device").isin("phone", "desktop")) & (F.col("p.listener_id").isNotNull()), "left_semi")
    .filter(F.col("last_known_days_since_played_at").between(210, 595))
)

leaders = (
    qualified
    .filter(F.col("played_at_dense_rank") <= 2)
    .drop("played_at_dense_rank")
)

result = (
    leaders
    .groupBy("listener_id", "track_id")
    .pivot("device", ["phone", "speaker", "car"])
    .agg(F.min("min_played_at_dense_rank_so_far"))
    .orderBy("listener_id", "track_id")
)
