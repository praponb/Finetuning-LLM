from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-06-03").cast("date"), F.lit("2025-08-02").cast("date"))) | (F.col("bpm") < 127))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
    )
)

exploded_detail_plays = (
    prepared.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.moods",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    exploded_detail_plays
    .cube("play_id", "artist_id", "play_source")
    .agg(
        F.min("release_date").alias("first_release_date"),
        F.countDistinct("play_id").alias("distinct_play_ids"),
        F.array_sort(F.collect_set("play_source")).alias("play_source_list"),
    )
)
