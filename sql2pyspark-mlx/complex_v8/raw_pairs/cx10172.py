from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.col("duration_sec") > 144))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        F.when(F.col("bpm") >= 156, "HIGH").when(F.col("bpm") >= 89, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

stats = (
    recent
    .groupBy("bpm_tier", "is_explicit")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

enriched = (
    stats
    .select(
        "*",
        F.coalesce(F.col("distinct_ids"), F.lit(0)).alias("distinct_ids_filled"),
        F.greatest(F.col("distinct_ids"), F.col("row_count")).alias("greatest_distinct_ids_row_count"),
    )
)

scored = (
    enriched
    .withColumn("least_row_count_distinct_ids", F.least(F.col("row_count"), F.col("distinct_ids")))
)

result = (
    scored
    .select(
        "bpm_tier",
        "row_count",
        "distinct_ids_filled",
        "greatest_distinct_ids_row_count",
        "least_row_count_distinct_ids",
        F.coalesce(F.col("distinct_ids_filled"), F.lit(0)).alias("distinct_ids_filled_filled"),
    )
    .orderBy("row_count")
)
