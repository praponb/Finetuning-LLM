from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter((F.col("device").isin("tv", "speaker", "car")) & (F.hour(F.col("played_at")).between(6, 9)))
    .select(
        "ms_played",
        F.col("played_at").alias("happened_at"),
        F.col("device").alias("name"),
    )
)

play_rows_2 = df_plays.select("track_id", "played_at", "source")

result = (
    play_rows
    .union(play_rows_2)
    .distinct()
)
