from pyspark.sql import functions as F

play_rows = df_plays.select(F.col("track_id").alias("entity_id"))

track_rows = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("release_date").between(F.lit("2024-05-12").cast("date"), F.lit("2025-08-02").cast("date"))))
    .select("id")
)

result = (
    play_rows
    .subtract(track_rows)
    .orderBy("entity_id")
)
