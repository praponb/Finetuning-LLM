from pyspark.sql import functions as F

artist_side = (
    df_artists
    .filter(F.col("debut_year") < 2015)
    .select(
        "genre",
        F.col("country").alias("category"),
        F.lit("artist").alias("source"),
    )
)

play_rows = df_plays.select("source", "device", F.lit("play").alias("source"))

listener_rows = (
    df_listeners
    .filter((F.lower(F.col("username")).like("%a9%")) & (F.col("signup_date") >= F.lit("2024-10-07").cast("date")))
    .select("username", "country", F.lit("listener").alias("source"))
)

result = (
    artist_side
    .union(play_rows)
    .union(listener_rows)
)
