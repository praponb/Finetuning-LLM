from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.array_contains(F.col("moods"), "chill")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

exploded_detail_plays = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    exploded_detail_plays
    .groupBy("play_source", "play_is_skipped")
    .pivot("play_device", ["car", "phone", "tv", "speaker"])
    .agg(F.max("bpm").alias("max_v"), F.sum("bpm").alias("sum_v"))
    .orderBy("play_source", "play_is_skipped")
)
