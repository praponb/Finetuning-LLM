from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(2, 3))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("device").isin("speaker", "phone"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        F.when(F.col("device").isin("desktop", "tv"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group_2"),
    )
)

totals = (
    scoped
    .groupBy("listener_id", "track_id", "device")
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.sum("ms_played").alias("total_ms_played"),
        F.min("played_at").alias("first_played_at"),
    )
    .filter(F.col("total_ms_played") > 99376)
)

with_children = (
    totals.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") < 225733), "inner")
    .select(
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.total_ms_played",
        "s.first_played_at",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

screened = (
    with_children.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.play_listener_id")) & (~F.col("p.device").isin("tv", "speaker")) & (F.col("p.ms_played") < 333795), "left_semi")
)

result = (
    screened
    .groupBy("listener_id", "play_id")
    .pivot("device", ["desktop", "phone", "tv"])
    .agg(F.sum("total_ms_played"))
    .orderBy("listener_id", "play_id")
)
