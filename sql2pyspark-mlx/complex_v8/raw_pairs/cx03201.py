from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter(F.col("country") != "TH")
    .select("id")
)

track_rows = df_tracks.select("id")

result = (
    listener_side
    .subtract(track_rows)
    .orderBy("id")
)
