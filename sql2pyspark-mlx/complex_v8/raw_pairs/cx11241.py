from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .select(
        F.lit("listener").alias("record_type"),
        "id",
        F.col("birth_year").alias("key_id"),
    )
)

play_side = (
    df_plays
    .filter(F.col("listener_id") >= 18)
    .select(F.lit("play").alias("record_type"), "id", "ms_played")
)

result = (
    listener_rows
    .union(play_side)
    .distinct()
    .orderBy("key_id", "id")
)
