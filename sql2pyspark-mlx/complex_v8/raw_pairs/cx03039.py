from pyspark.sql import functions as F

track_side = df_tracks.select("id", "release_date")

track_side_2 = df_tracks.select("bpm", "release_date")

listener_rows = (
    df_listeners
    .filter((F.col("country") == "BR") & (F.col("birth_year") < 1994))
    .select("birth_year", "signup_date")
)

result = (
    track_side
    .union(track_side_2)
    .union(listener_rows)
    .orderBy("release_date", "id")
)
