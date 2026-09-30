from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter(F.col("signup_date").isNotNull())
    .select(
        F.col("birth_year").alias("qty"),
        "username",
        F.lit("listener").alias("src"),
    )
)

track_side = (
    df_tracks
    .filter((F.col("artist_id") >= 3) & (F.array_contains(F.col("moods"), "workout")))
    .select("duration_sec", "title", F.lit("track").alias("src"))
)

result = (
    listener_side
    .union(track_side)
    .distinct()
    .orderBy("qty", "src")
)
