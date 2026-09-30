from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_by_bpm_running = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id")
w_is_explicit_by_bpm_ties = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc())

filtered_tracks = (
    df_tracks
    .filter(F.col("duration_sec") >= 142)
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
    )
)

deduped = (
    filtered_tracks
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

windowed = (
    deduped
    .select(
        "*",
        F.max("bpm").over(w_is_explicit_by_bpm_running).alias("max_bpm_so_far"),
        F.lead("bpm", 1, 0).over(w_is_explicit_by_bpm).alias("next_bpm"),
        F.sum("bpm").over(w_is_explicit_by_bpm_running).alias("running_bpm"),
        (F.col("bpm") - F.lag("bpm").over(w_is_explicit_by_bpm)).alias("bpm_change"),
        F.dense_rank().over(w_is_explicit_by_bpm_ties).alias("bpm_dense_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        "max_bpm_so_far",
        "next_bpm",
        "running_bpm",
        "bpm_change",
        "bpm_dense_rank",
        F.round(F.col("bpm_change") / F.nullif(F.col("next_bpm"), F.lit(0)), 3).alias("bpm_change_per_next_bpm"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
