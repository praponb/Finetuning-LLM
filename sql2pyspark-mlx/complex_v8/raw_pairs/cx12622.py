from pyspark.sql import functions as F

play_side = df_plays.select("listener_id", "device")

play_side_2 = (
    df_plays
    .filter((F.col("ms_played") < 324437) & (F.col("source").isin("radio", "search", "album")))
    .select("id", "source")
)

result = (
    play_side
    .exceptAll(play_side_2)
    .orderBy(F.col("listener_id").desc())
)
