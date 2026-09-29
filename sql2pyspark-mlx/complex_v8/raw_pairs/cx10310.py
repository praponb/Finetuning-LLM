from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")

filtered_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("source") == "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
        F.when(F.col("source").isin("playlist", "radio"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

with_counts = (
    filtered_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

metrics = (
    with_counts
    .select(
        "*",
        F.round(F.col("play_count") / F.nullif(F.sum("play_count").over(w_track_id_is_skipped), F.lit(0)), 4).alias("play_count_share_of_track_id_is_skipped"),
        F.sum("ms_played").over(w_track_id_is_skipped).alias("track_id_is_skipped_sum_ms_played"),
    )
)

result = (
    metrics
    .groupBy("listener_id")
    .pivot("source_group", ["group_a", "other", "group_b"])
    .agg(F.max("ms_played"))
    .orderBy("listener_id")
)
