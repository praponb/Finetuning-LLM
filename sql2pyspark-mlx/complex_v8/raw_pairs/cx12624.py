from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
    )
)

result = (
    scoped_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
