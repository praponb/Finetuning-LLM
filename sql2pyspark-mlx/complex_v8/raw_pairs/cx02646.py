from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((F.array_contains(F.col("interests"), "kpop")) & (F.upper(F.trim(F.col("username"))).like("F%")))
    .select(
        "id",
        F.col("signup_date").alias("event_date"),
        "birth_year",
        F.lit("listener").alias("src"),
    )
)

listener_side_2 = (
    df_listeners
    .select(
        "birth_year",
        "signup_date",
        "id",
        F.lit("listener_2").alias("src"),
    )
)

track_rows = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("R%"))
    .select(
        "bpm",
        "release_date",
        "artist_id",
        F.lit("track").alias("src"),
    )
)

result = (
    listener_side
    .union(listener_side_2)
    .union(track_rows)
)
