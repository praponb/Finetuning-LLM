from pyspark.sql import functions as F

recent = df_listeners.filter(F.col("signup_date") < F.lit("2025-01-29").cast("date"))

listener_rows = (
    recent
    .filter(F.array_contains(F.col("interests"), "live"))
    .select("id", F.col("birth_year").alias("ref_id"))
)

track_side = df_tracks.select("duration_sec", "id")

play_side = df_plays.select("ms_played", "track_id")

all_events = (
    listener_rows
    .union(track_side)
    .union(play_side)
)

result = (
    all_events
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("id")
)
