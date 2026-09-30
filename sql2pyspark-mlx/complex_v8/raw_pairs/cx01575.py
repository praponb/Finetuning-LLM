from pyspark.sql import functions as F

track_rows = df_tracks.select(F.col("duration_sec").alias("entity_id"))

listener_rows = (
    df_listeners
    .filter((F.col("signup_date") >= F.lit("2024-10-17").cast("date")) & (F.col("country") == "KR"))
    .select("id")
)

result = track_rows.subtract(listener_rows)
