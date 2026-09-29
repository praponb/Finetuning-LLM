from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 151112).alias("ms_played_distance"),
    )
)

detail_rows = (
    prepared_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.ms_played_distance",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

result = (
    detail_rows
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "track_id",
        "source",
        "ms_played_distance",
        "play_id",
        "play_played_at",
    )
    .orderBy(F.col("track_id").desc(), F.col("source").desc_nulls_first())
)
