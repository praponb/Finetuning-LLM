from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_to_date_source = Window.partitionBy(F.to_date("played_at"), "source")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_device_by_ms_played = Window.partitionBy("device").orderBy("ms_played", "id")
w_play_count_2_play_count_by_p = Window.partitionBy("play_count_2", "play_count").orderBy("played_at")
w_device_distinct_listener_id = Window.partitionBy("device", "distinct_listener_id_in_played_at_to_date_source").orderBy(F.col("play_count").desc())
w_play_count_device_by_ms_play = Window.partitionBy("play_count", "device").orderBy("ms_played_change", "id").rowsBetween(-3, -1)
w_play_count_device_by_ms_play_2 = Window.partitionBy("play_count", "device").orderBy("ms_played_change", "id")

base_plays = (
    df_plays
    .filter(F.col("source") == "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

with_counts = (
    base_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.min_play_ms_played",
    )
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(F.count("*").alias("play_count_2"))
)

with_stats = (
    with_counts.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

with_trends = (
    with_stats
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "play_count",
        "play_count_2",
        F.max("ms_played").over(w_played_at_to_date_source).alias("played_at_to_date_source_max_ms_played"),
        F.row_number().over(w_all_by_played_at).alias("rn"),
        (F.col("ms_played") - F.lag("ms_played").over(w_device_by_ms_played)).alias("ms_played_change"),
        F.size(F.collect_set("listener_id").over(w_played_at_to_date_source)).alias("distinct_listener_id_in_played_at_to_date_source"),
        F.round(F.cume_dist().over(w_play_count_2_play_count_by_p), 3).alias("played_at_cume_dist"),
        F.round(F.col("min_play_ms_played") / F.nullif(F.sum("min_play_ms_played").over(w_played_at_to_date_source), F.lit(0)), 4).alias("min_play_ms_played_share_of_played_at_to_date_so"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "listener_id",
        "track_id",
        "play_count_2",
        "played_at_to_date_source_max_ms_played",
        "distinct_listener_id_in_played_at_to_date_source",
        "played_at_cume_dist",
        "min_play_ms_played_share_of_played_at_to_date_so",
        F.rank().over(w_device_distinct_listener_id).alias("play_count_rank"),
        F.max("played_at_cume_dist").over(w_play_count_device_by_ms_play).alias("played_at_cume_dist_max_3_prior"),
        F.round(F.percent_rank().over(w_play_count_device_by_ms_play_2), 3).alias("ms_played_change_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "play_count_2",
        "distinct_listener_id_in_played_at_to_date_source",
        "played_at_cume_dist",
        "min_play_ms_played_share_of_played_at_to_date_so",
        "play_count_rank",
        "played_at_cume_dist_max_3_prior",
        "ms_played_change_percent_rank",
        F.round(F.col("played_at_cume_dist") * 0.9, 2).alias("played_at_cume_dist_adj"),
        F.when(F.col("play_count_rank") >= 7, "large").when(F.col("play_count_rank") >= 5, "medium").otherwise("small").alias("play_count_rank_tier"),
    )
    .orderBy(
        F.col("played_at_cume_dist_max_3_prior").asc_nulls_last(),
        F.col("play_count_rank_tier").desc(),
        "id",
    )
    .limit(20)
)
