from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("device") != "phone")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.year(F.col("played_at")).alias("played_at_year"),
        F.when(F.col("device").isin("car", "desktop"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

summarized_rows = (
    base.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device").isin("tv", "desktop", "car")), "left_semi")
    .filter(F.col("source") != "playlist")
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left_semi")
    .filter(F.col("played_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp"))
)

result = (
    with_activity_2
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "device_group",
        "source_ref",
        "play_count",
        F.date_trunc("week", F.col("played_at")).alias("played_at_week"),
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
    )
)
