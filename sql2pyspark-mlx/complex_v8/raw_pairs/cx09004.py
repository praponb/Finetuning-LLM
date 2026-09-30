from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")) & (F.col("source") == "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

play_side = (
    scoped_plays
    .filter(F.col("device") == "speaker")
    .select(F.col("track_id").alias("qty"))
)

play_side_2 = (
    df_plays
    .filter((F.to_date(F.col("played_at")) < F.lit("2025-02-12").cast("date")) & (F.col("device") == "tv"))
    .select("ms_played")
)

result = (
    play_side
    .subtract(play_side_2)
    .orderBy("qty")
)
