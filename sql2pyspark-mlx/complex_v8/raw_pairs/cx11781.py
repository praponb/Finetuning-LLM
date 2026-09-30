from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("track_id", "source").orderBy(F.col("ms_played").desc(), F.col("id").desc())

recent = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp")) & (~F.col("is_skipped")))
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

deduped = (
    recent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.max("ms_played").alias("max_play_ms_played"),
    )
)

with_counts = (
    deduped.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.last_played_at", "p.max_play_ms_played")
)

result = (
    with_counts
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source",
        "last_played_at",
        "max_play_ms_played",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("last_played_at").asc_nulls_last(),
        "id",
    )
    .limit(20)
)
