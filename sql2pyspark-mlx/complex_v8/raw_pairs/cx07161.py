from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_bpm_tier_is_explicit = Window.partitionBy("bpm_tier", "is_explicit")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec", "id")

cleaned = (
    df_tracks
    .filter((F.col("artist_id") <= 2) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

scored = (
    cleaned
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.upper(F.trim(F.col("title"))).alias("title_clean_2"),
        F.when(F.col("bpm") >= 163, "HIGH").when(F.col("bpm") >= 89, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
    )
)

enriched = scored.withColumn("bpm_tier_clean", F.upper(F.trim(F.col("bpm_tier"))))

sequenced = (
    enriched
    .select(
        "*",
        F.nth_value("bpm", 2).over(w_is_explicit_by_duration_sec).alias("second_bpm"),
        F.sum("bpm").over(w_bpm_tier_is_explicit).alias("bpm_tier_is_explicit_sum_bpm"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_is_explicit_by_duration_sec_2)).alias("duration_sec_change"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "title_clean",
        "title_clean_2",
        "bpm_tier",
        "second_bpm",
        "bpm_tier_is_explicit_sum_bpm",
        "duration_sec_change",
        F.when(F.col("duration_sec") >= 352, "HIGH").when(F.col("duration_sec") >= 165, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
    )
)
