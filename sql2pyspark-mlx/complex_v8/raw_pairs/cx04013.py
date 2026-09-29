from pyspark.sql import functions as F

base_listeners = (
    df_listeners
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
    )
)

listener_side = (
    base_listeners
    .filter(F.col("birth_year") >= 1970)
    .select(
        "birth_year",
        F.col("signup_date").alias("event_date"),
        F.lit("listener").alias("src"),
    )
)

track_side = df_tracks.select("artist_id", "release_date", F.lit("track").alias("src"))

unioned = listener_side.union(track_side)

result = (
    unioned
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("birth_year").alias("total_birth_year"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("src")
)
