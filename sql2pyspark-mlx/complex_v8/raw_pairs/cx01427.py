from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .filter(F.year(F.col("signup_date")) == 2024)
    .select(F.col("country").alias("label"))
)

track_rows = (
    df_tracks
    .filter((F.col("bpm") < 166) & (F.col("artist_id").isNotNull()))
    .select("title")
)

result = (
    listener_rows
    .subtract(track_rows)
    .orderBy(F.col("label").desc())
)
