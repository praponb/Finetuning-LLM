from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())

cleaned_tracks = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("R%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

deduped = (
    cleaned_tracks
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .groupingSets(
        [["is_explicit", "artist_id"], ["is_explicit"]],
        "is_explicit", "artist_id",
    )
    .agg(
        F.percentile_approx("days_since_release_date", 0.5).alias("median_days_since_release_date"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
