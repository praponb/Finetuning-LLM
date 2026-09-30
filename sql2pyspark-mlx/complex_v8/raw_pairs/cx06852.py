from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .filter((F.array_contains(F.col("interests"), "podcasts")) & (F.col("birth_year") >= 1966))
    .select(F.col("birth_year").alias("ref_id"))
)

track_side = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select("bpm")
)

result = listener_rows.subtract(track_side)
