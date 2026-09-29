from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy("duration_sec", "id")

cleaned = (
    df_tracks
    .filter(F.col("artist_id") >= 8)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.when(F.col("bpm") >= 155, "large").when(F.col("bpm") >= 94, "medium").otherwise("small").alias("bpm_tier"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

enriched_artist = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.moods_text",
        "s.bpm_tier",
        "s.bpm_filled",
        F.col("a.country").alias("artist_country"),
    )
)

unique_rows = (
    enriched_artist
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .rollup("artist_id", "artist_country")
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.sum("bpm_filled"), F.lit(0)), 3).alias("duration_sec_to_bpm_filled_ratio"),
    )
    .select(
        "artist_id",
        F.coalesce(F.col("artist_country"), F.lit("ALL")).alias("artist_country"),
        "duration_sec_to_bpm_filled_ratio",
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
    )
)
