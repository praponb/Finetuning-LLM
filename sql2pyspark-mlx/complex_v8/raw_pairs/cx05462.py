from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .filter((F.array_contains(F.col("interests"), "live")) & (~F.col("plan").isin("premium", "student")))
    .select("username")
)

track_side = (
    df_tracks
    .filter(F.col("release_date").isNotNull())
    .select("title")
)

result = (
    listener_rows
    .subtract(track_side)
    .orderBy("username")
)
