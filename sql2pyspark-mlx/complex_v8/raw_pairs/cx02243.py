from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_is_skipped_by_ms_play = Window.partitionBy("device", "is_skipped").orderBy("ms_played", "id", "play_id")
w_device_is_skipped_by_ms_play_2 = Window.partitionBy("device", "is_skipped").orderBy("ms_played")
w_play_id_play_device = Window.partitionBy("play_id", "play_device")

prepared_plays = (
    df_plays
    .filter((F.col("device") == "car") & (F.col("listener_id") <= 11))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.abs(F.col("ms_played") - 112786).alias("ms_played_distance"),
        F.month(F.col("played_at")).alias("played_at_month"),
        F.when(F.col("device").isin("tv", "phone"), "group_a").when(F.col("device") == "desktop", "group_b").otherwise("other").alias("device_group"),
    )
)

detail_rows_plays = (
    prepared_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device").isin("phone", "speaker", "desktop")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.device_group",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

windowed = (
    detail_rows_plays
    .select(
        "*",
        (F.col("play_ms_played") - F.lag("play_ms_played").over(w_device_is_skipped_by_ms_play)).alias("play_ms_played_change"),
        F.dense_rank().over(w_device_is_skipped_by_ms_play_2).alias("ms_played_dense_rank"),
        F.max("ms_played").over(w_play_id_play_device).alias("play_id_play_device_max_ms_played"),
    )
)

result = (
    windowed
    .cube("play_device", "is_skipped", "device")
    .agg(
        F.grouping("play_device").alias("is_total_play_device"),
        F.count("*").alias("cnt"),
        F.round(F.avg("play_ms_played_change"), 2).alias("avg_play_ms_played_change"),
    )
)
