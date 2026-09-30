from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())

prepared_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

latest = (
    prepared_tracks
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

with_stats = (
    latest.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

result = (
    with_stats
    .cube("play_count", "is_explicit")
    .agg(
        F.sum(F.when(~F.col("is_explicit"), F.col("bpm"))).alias("bpm_not_is_explicit"),
    )
    .orderBy(
        F.col("play_count").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
