from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter((F.col("source").isin("album", "search", "radio")) & (F.col("ms_played") > 67932))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("track_id").cast("string")).alias("device_ref"),
    )
)

play_side = (
    cleaned_plays
    .filter((F.col("track_id") >= 21) & (F.hour(F.col("played_at")).between(6, 10)))
    .select("ms_played", "played_at", F.col("device").alias("category"))
)

play_side_2 = df_plays.select("ms_played", "played_at", "source")

all_events = (
    play_side
    .union(play_side_2)
    .distinct()
)

result = (
    all_events
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ms_played").alias("total_ms_played"),
        F.max("played_at").alias("latest_played_at"),
    )
    .orderBy("category")
)
