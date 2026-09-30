from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc(), F.col("id").desc(), F.col("play_id").desc())

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

enriched = (
    scoped_plays
    .select(
        "*",
        F.initcap(F.trim(F.col("source_device_label"))).alias("source_device_label_clean"),
    )
)

exploded_detail = (
    enriched.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.source_device_label",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
    )
)

with_activity = (
    exploded_detail.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.source").isin("playlist", "album", "radio")), "left_anti")
    .filter(F.col("is_skipped"))
)

deduped = (
    with_activity
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "listener_id",
        "track_id",
        "source_device_label",
        "play_id",
        "play_is_skipped",
        "play_device",
    )
    .orderBy("listener_id", F.col("play_id").asc_nulls_last(), "id")
    .limit(10)
)
