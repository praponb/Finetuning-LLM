from pyspark.sql import functions as F

recent = (
    df_plays
    .filter((F.col("ms_played") < 302279) & (F.col("listener_id") <= 12))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

play_rows = (
    recent
    .select(
        F.col("ms_played").alias("qty"),
        "played_at",
        "device",
        F.lit("play").alias("src"),
    )
)

play_rows_2 = (
    df_plays
    .filter((F.col("source").isin("playlist", "radio")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "played_at",
        "device",
        F.lit("play_2").alias("src"),
    )
)

play_rows_3 = (
    df_plays
    .select(
        "ms_played",
        "played_at",
        "device",
        F.lit("play_3").alias("src"),
    )
)

stacked = (
    play_rows
    .union(play_rows_2)
    .union(play_rows_3)
)

result = (
    stacked
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("played_at").alias("latest_played_at"),
    )
    .orderBy("src")
)
