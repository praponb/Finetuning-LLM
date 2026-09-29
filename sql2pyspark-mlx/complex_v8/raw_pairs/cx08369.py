from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("release_date_month").desc(), F.col("id").desc())

valid_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 352)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

deduped = (
    valid_tracks
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .groupingSets(
        [["release_date_month", "artist_id"], ["release_date_month"], ["artist_id"]],
        "release_date_month", "artist_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("bpm", 0.5).alias("median_bpm"),
        F.round(F.stddev("release_date_month"), 2).alias("stddev_release_date_month"),
    )
    .orderBy(
        F.col("release_date_month").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
