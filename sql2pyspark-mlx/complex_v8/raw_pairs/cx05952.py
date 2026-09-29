from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_played_at_month_source = Window.partitionBy(F.month("play_played_at"), "source").orderBy("ms_played", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_play_played_at_ties = Window.orderBy("play_played_at")
w_all_by_play_played_at_moving = Window.orderBy("play_played_at", "id", "play_id").rowsBetween(-2, Window.currentRow)

prepared = (
    df_plays
    .filter(F.col("source") == "album")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.abs(F.col("ms_played") - 157900).alias("ms_played_distance"),
    )
)

exploded_detail = (
    prepared.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.source",
        "s.ms_played_filled",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

with_windows = (
    exploded_detail
    .select(
        "id",
        "listener_id",
        "ms_played",
        "source",
        "ms_played_filled",
        "play_id",
        "play_played_at",
        "play_device",
        F.last("ms_played_filled").over(w_play_played_at_month_source).alias("last_ms_played_filled"),
        F.rank().over(w_all_by_play_played_at_ties).alias("play_played_at_rank"),
        F.max("ms_played_filled").over(w_all_by_play_played_at_moving).alias("ms_played_filled_max_3"),
    )
)

joined_listener = (
    with_windows.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.*",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.country").alias("listener_country"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    joined_listener
    .groupBy("play_device", "source")
    .pivot("listener_country", ["TH", "BR"])
    .agg(F.min("ms_played_filled_max_3").alias("min_v"), F.sum("ms_played_filled_max_3").alias("sum_v"))
    .orderBy("play_device", "source")
)
