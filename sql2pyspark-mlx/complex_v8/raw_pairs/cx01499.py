from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy("release_date", "id")

cleaned_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("G%")) & (F.col("release_date").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

latest = (
    cleaned_tracks
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

summarized_rows = (
    latest.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "release_date_plus_12m",
        "duration_sec_per_bpm",
        "play_count",
        "play_is_skipped_count",
        F.date_format(F.col("release_date_plus_12m"), "MMM yyyy").alias("release_date_plus_12m_label"),
        F.round(F.col("play_count") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("play_count_per_bpm"),
    )
    .orderBy(F.col("release_date").asc())
)
