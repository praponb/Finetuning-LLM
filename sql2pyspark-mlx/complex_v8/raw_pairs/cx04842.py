from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("listener_id", "source").orderBy(F.col("is_skipped_flag").desc(), F.col("id").desc())

scoped_plays = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(8, 13))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

deduped = (
    scoped_plays
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_children_plays = (
    deduped.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.source",
        "s.ms_played_bucket",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    with_children_plays
    .cube("play_device", "source")
    .agg(
        F.min("ms_played_bucket").alias("min_ms_played_bucket"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("play_device"), F.lit("ALL")).alias("play_device"),
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "min_ms_played_bucket",
        "row_count",
    )
    .orderBy(F.col("play_device").asc_nulls_last(), F.col("source").asc_nulls_last())
)
