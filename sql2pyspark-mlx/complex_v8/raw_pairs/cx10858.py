from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id")
w_artist_id_is_explicit_by_bpm_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm")

prepared_tracks = (
    df_tracks
    .filter(F.col("duration_sec").between(144, 409))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

unique_rows = (
    prepared_tracks
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_trends = (
    unique_rows
    .select(
        "*",
        F.round(F.cume_dist().over(w_is_explicit_by_bpm), 3).alias("bpm_cume_dist"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_is_explicit_by_bpm)).alias("bpm_change"),
        F.round(F.cume_dist().over(w_artist_id_is_explicit_by_bpm), 3).alias("bpm_cume_dist_2"),
        F.rank().over(w_artist_id_is_explicit_by_bpm_2).alias("bpm_rank"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_by_bpm)).alias("days_since_prev_release_date"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "bpm_cume_dist",
        "bpm_cume_dist_2",
        "days_since_prev_release_date",
    )
)
