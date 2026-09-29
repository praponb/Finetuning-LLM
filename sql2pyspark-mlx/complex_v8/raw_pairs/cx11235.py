from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy("first_track_release_date", "id")

prepared = (
    df_tracks
    .filter((F.col("duration_sec").between(164, 409)) & (F.col("bpm") <= 163))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.min("release_date").alias("first_track_release_date"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.first_track_release_date",
    )
)

artist_id_stats = (
    with_counts
    .groupBy("artist_id")
    .agg(F.max("bpm").alias("max_bpm"))
)

outliers = (
    with_counts.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") == F.col("g.max_bpm"))
    .select(
        "s.*",
        "g.max_bpm",
        F.round(F.col("s.bpm") - F.col("g.max_bpm"), 2).alias("bpm_minus_max"),
    )
)

latest_per_key = (
    outliers
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

derived = (
    latest_per_key
    .filter(F.col("first_track_release_date") >= F.lit("2024-07-01").cast("date"))
    .select(
        "*",
        F.when(F.col("bpm_minus_max") >= 54.0, "HIGH").when(F.col("bpm_minus_max") >= -108.0, "MEDIUM").otherwise("LOW").alias("bpm_minus_max_tier"),
        F.round(F.col("bpm_minus_max") * 0.9, 2).alias("bpm_minus_max_adj"),
    )
)

result = (
    derived
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "track_count",
        "first_track_release_date",
        "max_bpm",
    )
)
