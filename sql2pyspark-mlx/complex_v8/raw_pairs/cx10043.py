from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_device_source_group_by_playe = Window.partitionBy("device", "source_group").orderBy(F.col("played_at").desc(), "id")

scoped = (
    df_plays
    .filter((F.col("device").isin("desktop", "tv", "car")) & (F.col("track_id") >= 21))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.concat(F.col("device"), F.lit("#"), F.col("id").cast("string")).alias("device_ref"),
        F.when(F.col("source").isin("playlist", "album"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
    )
)

with_trends = (
    scoped
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device_ref",
        "source_group",
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_listener_id_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
        F.round(F.avg("ms_played").over(w_listener_id), 2).alias("listener_id_avg_ms_played"),
        F.lag("ms_played").over(w_device_source_group_by_playe).alias("prev_ms_played"),
    )
)

exploded_detail = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.device_ref",
        "s.ms_played_share_of_listener_id",
        "s.minutes_since_prev_played_at",
        "s.listener_id_sum_ms_played",
        "s.listener_id_avg_ms_played",
        "s.prev_ms_played",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

result = (
    exploded_detail
    .cube("play_source", "play_is_skipped", "play_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("play_device").isin("desktop", "phone"), 1)).alias("n_play_device_in_desktop_phone"),
    )
    .orderBy(
        F.col("play_source").asc_nulls_last(),
        F.col("play_is_skipped").asc_nulls_last(),
        F.col("play_id").asc_nulls_last(),
    )
)
