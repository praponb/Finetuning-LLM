from pyspark.sql import functions as F

listener_side = df_listeners.select("id", F.col("birth_year").alias("ref_id"), "signup_date")

track_side = (
    df_tracks
    .filter(F.col("release_date").isNotNull())
    .select(
        F.col("bpm").alias("id"),
        F.col("artist_id").alias("ref_id"),
        F.col("release_date").alias("signup_date"),
    )
)

stacked = (
    listener_side
    .unionByName(track_side)
    .distinct()
)

result = (
    stacked
    .groupBy("id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("signup_date").alias("latest_signup_date"),
    )
    .orderBy("id")
)
