from pyspark.sql import functions as F

track_rows = (
    df_tracks
    .filter(F.col("artist_id") >= 11)
    .select("bpm", F.col("artist_id").alias("qty"), F.lit("track").alias("origin"))
)

play_side = (
    df_plays
    .select(
        F.col("id").alias("bpm"),
        F.col("track_id").alias("qty"),
        F.lit("play").alias("origin"),
    )
)

combined = track_rows.unionByName(play_side)

result = (
    combined
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("bpm").alias("total_bpm"))
    .orderBy("origin")
)
