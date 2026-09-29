from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").desc(), F.col("id").desc())

base_plays = (
    df_plays
    .filter((~F.col("device").isin("desktop", "phone")) & (F.col("listener_id") <= 14))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

filtered = (
    base_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (~F.col("p.is_skipped")), "left_anti")
)

enriched = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.source",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
        F.col("t.title").alias("track_title"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

unique_rows = (
    enriched
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .groupBy("track_id", "listener_id")
    .pivot("source", ["radio", "album"])
    .agg(F.min("track_duration_sec"))
)
