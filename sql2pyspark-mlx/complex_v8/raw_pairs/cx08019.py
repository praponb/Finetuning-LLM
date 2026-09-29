from pyspark.sql import functions as F

artist_side = df_artists.select(F.col("id").alias("ref_id"))

listener_rows = df_listeners.select(F.col("id").alias("ref_id"))

result = artist_side.exceptAll(listener_rows)
