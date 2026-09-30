from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())

base_tracks = (
    df_tracks
    .filter((F.col("duration_sec") < 365) & (F.lower(F.col("title")).like("%on%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

deduped = (
    base_tracks
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

without_matches = (
    deduped.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "title_prefix",
        F.when(F.col("duration_sec") >= 352, "large").when(F.col("duration_sec") >= 213, "medium").otherwise("small").alias("duration_sec_tier"),
    )
    .orderBy(F.col("duration_sec_tier").desc())
)
