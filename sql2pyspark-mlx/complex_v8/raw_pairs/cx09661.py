from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())

scoped = (
    df_tracks
    .filter((F.col("duration_sec").between(183, 352)) & (~F.array_contains(F.col("moods"), "sad")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
)

unique_rows = (
    scoped
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .filter(F.col("artist_id") <= 7)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title_clean")), 1, 2).alias("title_clean_prefix"),
    )
    .orderBy(F.col("duration_sec").asc(), F.col("id").asc_nulls_last())
)
