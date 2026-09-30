from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("listener_id").orderBy(F.col("play_count").desc(), F.col("id").desc())

recent = (
    df_plays
    .filter((F.col("device").isin("desktop", "speaker")) & (F.col("source") == "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_stats = (
    recent.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
    )
)

unique_rows = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    unique_rows.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.play_count",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    enriched
    .cube("listener_plan", "is_skipped", "listener_id")
    .agg(F.sum("play_count").alias("total_play_count"))
    .select(
        F.coalesce(F.col("listener_plan"), F.lit("ALL")).alias("listener_plan"),
        "is_skipped",
        "listener_id",
        "total_play_count",
    )
)
