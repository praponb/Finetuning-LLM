from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter(F.col("ms_played") >= 99376)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
    )
)

exploded_detail = (
    cleaned_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "listener_id",
        "track_id",
        "source",
        "play_id",
        "play_is_skipped",
        "play_listener_id",
        F.concat_ws("|", F.col("source"), F.col("play_source")).alias("source_play_source_label"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
    .orderBy(F.col("source_slug").asc_nulls_last(), "source")
)
