from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("track_id", "device").orderBy(F.col("track_bpm").desc(), F.col("id").desc())

filtered = (
    df_plays
    .filter((F.col("ms_played").between(108979, 241585)) | (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count("*").alias("play_count_2"),
        F.count("*").alias("play_count_3"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
        F.coalesce(F.col("p.play_count_3"), F.lit(0)).alias("play_count_3"),
    )
)

with_parent_track = (
    summarized_rows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.play_count",
        "s.play_count_2",
        "s.play_count_3",
        F.col("t.bpm").alias("track_bpm"),
    )
)

latest_per_key = (
    with_parent_track
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        "play_count",
        "play_count_2",
        "play_count_3",
        "track_bpm",
        F.when(F.col("source").isin("album", "search"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
    )
    .orderBy("listener_id", F.col("device").asc(), "id")
    .limit(5)
)
