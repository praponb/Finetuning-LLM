from pyspark.sql import functions as F

track_rows = (
    df_tracks
    .filter((F.col("release_date").isNotNull()) & (F.col("bpm") <= 142))
    .select("artist_id", "release_date", F.lit("track").alias("record_type"))
)

listener_side = df_listeners.select("id", "signup_date", F.lit("listener").alias("record_type"))

listener_side_2 = (
    df_listeners
    .filter(F.upper(F.trim(F.col("username"))).like("D%"))
    .select("id", "signup_date", F.lit("listener").alias("record_type"))
)

result = (
    track_rows
    .union(listener_side)
    .union(listener_side_2)
    .orderBy(F.col("artist_id").desc())
)
