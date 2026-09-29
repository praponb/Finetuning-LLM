from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter((F.hour(F.col("played_at")).between(7, 12)) & (F.col("is_skipped")))
    .select(F.col("source").alias("label"), "device")
)

listener_rows = df_listeners.select("plan", "username")

result = (
    play_side
    .union(listener_rows)
    .orderBy(F.col("device").desc())
)
