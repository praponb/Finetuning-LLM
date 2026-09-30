from pyspark.sql import functions as F

valid = (
    df_tracks
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

tagged = (
    valid
    .withColumn("bpm_tier", F.when(F.col("bpm") >= 142, "large").when(F.col("bpm") >= 89, "medium").otherwise("small"))
)

summary = (
    tagged
    .groupBy("bpm_tier", "is_explicit")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.percentile_approx("bpm", 0.5).alias("median_bpm"),
        F.min("bpm").alias("min_bpm"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .groupBy("cnt", "is_explicit")
    .pivot("bpm_tier", ["large", "medium"])
    .agg(F.max("median_bpm").alias("max_v"), F.sum("median_bpm").alias("sum_v"))
    .orderBy("cnt", "is_explicit")
)
