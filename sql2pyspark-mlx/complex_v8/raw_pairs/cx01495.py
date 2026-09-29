from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("track_id").orderBy(F.col("played_at").desc(), F.col("id").desc())

filtered_plays = (
    df_plays
    .filter((F.col("device").isin("tv", "desktop")) & (F.col("track_id") >= 2))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

unique_rows = (
    filtered_plays
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.count("*").alias("play_count"),
        F.count(F.when(F.col("device").isin("speaker", "car"), 1)).alias("play_device_count"),
    )
)

summarized_rows = (
    unique_rows.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

listener_id_stats = (
    summarized_rows
    .groupBy("listener_id")
    .agg(F.max("play_device_count").alias("max_play_device_count"))
)

outliers = (
    summarized_rows.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.play_device_count") == F.col("g.max_play_device_count"))
    .select(
        "s.*",
        "g.max_play_device_count",
        F.round(F.col("s.play_device_count") - F.col("g.max_play_device_count"), 2).alias("play_device_count_minus_max"),
    )
)

enriched = (
    outliers
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "play_count",
        "play_device_count",
        "max_play_device_count",
        "play_device_count_minus_max",
        F.when(F.col("max_play_device_count") >= 4.4, "A").when(F.col("max_play_device_count") >= 2.0, "B").otherwise("C").alias("max_play_device_count_tier"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
        (F.col("played_at") + F.expr("INTERVAL 30 MINUTES")).alias("played_at_plus_30m"),
    )
)

result = (
    enriched
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "play_device_count",
        "max_play_device_count",
        "max_play_device_count_tier",
        "played_at_date",
        "played_at_plus_30m",
    )
)
