from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter(F.col("listener_id") <= 3)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
    )
)

exploded_detail = (
    cleaned_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

combined_details_play = (
    exploded_detail.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.play_id",
        "s.play_source",
        "s.play_listener_id",
        "s.play_ms_played",
        F.col("p.track_id").alias("play_track_id"),
    )
)

result = (
    combined_details_play
    .filter(F.col("play_listener_id") <= 7)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "play_id",
        "play_source",
        "play_listener_id",
        "play_ms_played",
        "play_track_id",
        F.when(F.col("play_source").isin("search", "radio"), "group_a").when(F.col("play_source") == "album", "group_b").otherwise("other").alias("play_source_group"),
    )
    .orderBy(F.col("listener_id").desc_nulls_first(), "play_ms_played")
)
