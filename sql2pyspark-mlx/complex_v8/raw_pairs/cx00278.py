from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((F.col("country").isin("BR", "TH")) & (F.upper(F.trim(F.col("username"))).like("D%")))
    .select("id", "signup_date", F.lit("listener").alias("src"))
)

track_side = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-11-14").cast("date"))
    .select(
        F.col("duration_sec").alias("id"),
        F.col("release_date").alias("signup_date"),
        F.lit("track").alias("src"),
    )
)

result = (
    listener_side
    .unionByName(track_side)
    .distinct()
    .orderBy(F.col("id").desc(), "src")
)
