from pyspark.sql import functions as F

base = (
    df_plays
    .filter((~F.col("device").isin("phone", "speaker")) & (F.col("ms_played") > 73625))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
    )
)

play_rows = df_plays.select("source", F.col("device").alias("name"), "played_at")

play_rows_2 = df_plays.select("device", "source", "played_at")

result = (
    play_rows
    .union(play_rows_2)
    .distinct()
)
