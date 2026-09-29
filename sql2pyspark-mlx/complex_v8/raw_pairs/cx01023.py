from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())

prepared_tracks = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        F.array_contains(F.col("moods"), "chill").alias("has_chill"),
    )
)

deduped = (
    prepared_tracks
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupingSets(
        [["has_chill", "artist_id", "is_explicit"], ["has_chill"], ["artist_id"]],
        "has_chill", "artist_id", "is_explicit",
    )
    .agg(
        F.sum(F.when(F.lower(F.col("title")).like("%ho%"), 1).otherwise(0)).alias("n_lower_title_like_ho"),
    )
    .orderBy(
        F.col("has_chill").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
