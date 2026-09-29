from pyspark.sql import functions as F

track_rows = df_tracks.select(F.col("title").alias("name"))

listener_rows = (
    df_listeners
    .filter((F.size(F.col("interests")) >= 1) & (F.col("plan").isin("student", "family", "premium")))
    .select("plan")
)

result = track_rows.intersect(listener_rows)
