from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), F.col("id").desc())

scoped_tracks = (
    df_tracks
    .filter(F.col("duration_sec") <= 357)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

latest_per_key = (
    scoped_tracks
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

annotated = (
    latest_per_key
    .select(
        "*",
        F.when(F.col("bpm") >= 156, "A").when(F.col("bpm") >= 100, "B").otherwise("C").alias("bpm_tier"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

result = (
    annotated
    .groupingSets(
        [["bpm_tier", "is_explicit"], ["bpm_tier"]],
        "bpm_tier", "is_explicit",
    )
    .agg(
        F.grouping("bpm_tier").alias("is_total_bpm_tier"),
        F.round(100 * F.count(F.when(~F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_not_is_explicit"),
    )
    .orderBy(
        F.col("bpm_tier").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
