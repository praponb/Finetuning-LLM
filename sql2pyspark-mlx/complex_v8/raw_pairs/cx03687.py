from pyspark.sql import functions as F

artist_side = df_artists.select("id")

listener_rows = df_listeners.select("id")

result = (
    artist_side
    .exceptAll(listener_rows)
    .orderBy("id")
)
