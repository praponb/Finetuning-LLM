from pyspark.sql import functions as F

listener_rows = df_listeners.select("plan", "signup_date", "birth_year")

track_side = df_tracks.select("title", "release_date", "id")

result = (
    listener_rows
    .union(track_side)
    .distinct()
)
