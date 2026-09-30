from pyspark.sql import functions as F

base_plays = (
    df_plays
    .filter(F.col("ms_played") >= 97825)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.substring(F.trim(F.col("source")), 1, 2).alias("source_prefix"),
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.sum("ms_played").alias("total_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_stats = (
    base_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.total_play_ms_played", "p.last_played_at")
)

result = (
    with_stats
    .select("id", "ms_played", "total_play_ms_played")
    .unpivot(
        ids=["id"],
        values=["ms_played", "total_play_ms_played"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
