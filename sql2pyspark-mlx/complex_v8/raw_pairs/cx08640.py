from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .filter((F.col("plan").isin("premium", "student")) & (F.array_contains(F.col("interests"), "live")))
    .select(F.col("birth_year").alias("qty"))
)

track_side = (
    df_tracks
    .filter(F.col("bpm") > 89)
    .select("duration_sec")
)

result = (
    listener_rows
    .intersect(track_side)
    .orderBy("qty")
)
