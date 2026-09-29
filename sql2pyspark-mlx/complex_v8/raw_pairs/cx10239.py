from pyspark.sql import functions as F

track_side = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("P%")) & (F.col("duration_sec") < 357))
    .select("id")
)

listener_side = df_listeners.select(F.col("birth_year").alias("id"))

result = (
    track_side
    .intersect(listener_side)
    .orderBy(F.col("id").desc())
)
