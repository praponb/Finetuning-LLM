from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter(F.col("signup_date") >= F.lit("2024-08-02").cast("date"))
    .select(F.lit("listener").alias("source"), "id", "signup_date")
)

track_rows = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 2) & (F.col("title").isNotNull()))
    .select(
        F.lit("track").alias("source"),
        F.col("duration_sec").alias("id"),
        F.col("release_date").alias("signup_date"),
    )
)

listener_rows = (
    df_listeners
    .filter(F.size(F.col("interests")) >= 2)
    .select(
        F.lit("listener_3").alias("source"),
        F.col("birth_year").alias("id"),
        "signup_date",
    )
)

unioned = (
    listener_side
    .unionByName(track_rows)
    .unionByName(listener_rows)
)

result = (
    unioned
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("signup_date").alias("latest_signup_date"),
    )
    .orderBy("source")
)
