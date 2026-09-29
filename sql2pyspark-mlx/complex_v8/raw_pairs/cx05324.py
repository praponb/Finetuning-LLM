from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy("bpm", "id")

scoped = (
    df_tracks
    .filter(F.col("duration_sec") > 144)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

latest_per_key = (
    scoped
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .rollup("is_explicit", "artist_id")
    .agg(
        F.round(F.sum("bpm") / F.nullif(F.sum("duration_sec_per_bpm"), F.lit(0)), 3).alias("bpm_to_duration_sec_per_bpm_ratio"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
