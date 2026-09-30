from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")) & (F.col("listener_id").isNotNull()))
    .select(F.col("track_id").alias("ref_id"), "source")
)

listener_rows = (
    df_listeners
    .filter(F.size(F.col("interests")) >= 2)
    .select("birth_year", "plan")
)

result = play_side.intersect(listener_rows)
