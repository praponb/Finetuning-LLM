from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("S%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
    )
)

tagged = (
    cleaned_tracks
    .select(
        "*",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
        F.greatest(F.col("bpm"), F.col("duration_sec")).alias("greatest_bpm_duration_sec"),
        F.when(F.col("bpm") >= 147, "A").when(F.col("bpm") >= 85, "B").otherwise("C").alias("bpm_tier"),
    )
)

annotated = (
    tagged
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "bpm_tier",
        F.when(F.col("bpm_tier").isin("B", "A"), "group_a").when(F.col("bpm_tier") == "C", "group_b").otherwise("other").alias("bpm_tier_group"),
        F.when(F.col("bpm_tier").isin("A", "C"), "group_a").when(F.col("bpm_tier") == "B", "group_b").otherwise("other").alias("bpm_tier_group_2"),
    )
)

result = (
    annotated
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "bpm_tier_group",
        "bpm_tier_group_2",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.when(F.col("duration_sec") >= 352, "large").when(F.col("duration_sec") >= 183, "medium").otherwise("small").alias("duration_sec_tier"),
    )
    .orderBy(
        F.col("bpm_tier_group_2").asc(),
        F.col("duration_sec_bucket").desc_nulls_first(),
        "id",
    )
    .limit(5)
)
