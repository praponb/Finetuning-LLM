from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("source").orderBy("played_at", "id")

scoped = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(9, 15))
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

unique_rows = (
    scoped
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
    )
)

with_stats = (
    unique_rows.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "first_played_at",
        "play_count",
        F.when(F.col("play_count") >= 5, "HIGH").when(F.col("play_count") >= 3, "MEDIUM").otherwise("LOW").alias("play_count_tier"),
    )
    .orderBy(F.col("play_count").desc())
)
