from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_play_count_ties = Window.orderBy(F.col("play_count").asc_nulls_last())
w_play_source_listener_id = Window.partitionBy("play_source", "listener_id")

cleaned = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

exploded_detail_plays = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.source").isin("playlist", "search", "radio")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

summarized_rows = (
    exploded_detail_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

ranked = (
    summarized_rows
    .select(
        "id",
        "listener_id",
        "play_id",
        "play_listener_id",
        "play_source",
        "play_count",
        F.round(F.cume_dist().over(w_all_by_play_count_ties), 3).alias("play_count_cume_dist"),
        F.min("play_count").over(w_play_source_listener_id).alias("play_source_listener_id_min_play_count"),
    )
)

result = (
    ranked
    .filter(F.col("listener_id") >= 10)
    .select(
        "id",
        "listener_id",
        "play_listener_id",
        "play_source",
        "play_count",
        "play_count_cume_dist",
        "play_source_listener_id_min_play_count",
        F.round(F.col("play_count_cume_dist") * 1.1, 2).alias("play_count_cume_dist_adj"),
        F.when(F.col("play_count_cume_dist") >= 0.8, "HIGH").when(F.col("play_count_cume_dist") >= 0.2, "MEDIUM").otherwise("LOW").alias("play_count_cume_dist_tier"),
    )
    .orderBy(F.col("play_count_cume_dist_adj").desc(), "play_count_cume_dist")
)
