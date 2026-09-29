from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(F.lit("play").alias("record_type"), "id", "played_at")
)

play_side = (
    df_plays
    .select(
        F.lit("play_2").alias("record_type"),
        F.col("track_id").alias("id"),
        "played_at",
    )
)

result = (
    play_rows
    .unionByName(play_side)
    .distinct()
    .orderBy("record_type")
)
