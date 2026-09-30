from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())
w_release_date_year_is_explici = Window.partitionBy(F.year("release_date"), "is_explicit")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_all_by_bpm_ties = Window.orderBy("bpm")

prepared_tracks = (
    df_tracks
    .filter(F.col("bpm") >= 75)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
)

deduped = (
    prepared_tracks
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    deduped
    .select(
        "*",
        F.max("duration_sec").over(w_release_date_year_is_explici).alias("release_date_year_is_explicit_max_duration_sec"),
        F.ntile(3).over(w_all_by_bpm).alias("bpm_tercile"),
        F.row_number().over(w_all_by_bpm).alias("rn"),
        F.dense_rank().over(w_all_by_bpm_ties).alias("bpm_dense_rank"),
    )
)

result = (
    ranked
    .groupingSets(
        [["is_explicit", "artist_id"], ["is_explicit"], ["artist_id"]],
        "is_explicit", "artist_id",
    )
    .agg(F.min("rn").alias("min_rn"))
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
