from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy("duration_sec_distance", "id")

cleaned = (
    df_tracks
    .filter(F.col("duration_sec") > 213)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.abs(F.col("duration_sec") - 352).alias("duration_sec_distance"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("duration_sec").alias("max_track_duration_sec"),
        F.min("release_date").alias("first_track_release_date"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.max_track_duration_sec", "t.first_track_release_date")
)

latest = (
    summarized_rows
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

tagged = (
    latest
    .filter(F.col("duration_sec_distance") < 336)
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
    )
)

result = (
    tagged
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "duration_sec_distance",
        "max_track_duration_sec",
        "first_track_release_date",
        "days_since_release_date",
        "title_ref",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
    .orderBy(F.col("release_date").desc(), F.col("title").desc_nulls_first())
)
