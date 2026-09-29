from pyspark.sql import functions as F

cleaned = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("source").isin("playlist", "search")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device").isin("speaker", "car"), 1)).alias("play_device_count"),
        F.count(F.when(F.col("device").isNotNull(), 1)).alias("play_device_count_2"),
    )
)

with_counts = (
    cleaned.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
        F.coalesce(F.col("p.play_device_count_2"), F.lit(0)).alias("play_device_count_2"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "play_device_count_2",
        "play_device_count",
        "ms_played",
    )
    .unpivot(
        ids=["id"],
        values=["play_device_count_2", "play_device_count", "ms_played"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
