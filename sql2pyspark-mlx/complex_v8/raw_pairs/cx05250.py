from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

per_label = (
    prepared
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods_count",
        "title_len",
        "duration_sec_bucket",
        F.explode("moods").alias("mood"),
    )
)

stats = (
    per_label
    .groupBy("is_explicit")
    .agg(
        F.round(100 * F.count(F.when(F.col("title").isNotNull(), 1)) / F.count("*"), 1).alias("pct_title_is_not_null"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
    )
)

labeled = (
    stats
    .filter(~F.col("is_explicit"))
    .select(
        "is_explicit",
        "pct_title_is_not_null",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

result = (
    labeled
    .select(
        "is_explicit",
        "pct_title_is_not_null",
        "is_explicit_flag",
        F.abs(F.col("is_explicit_flag") - 0).alias("is_explicit_flag_distance"),
        (F.col("pct_title_is_not_null") - F.col("is_explicit_flag")).alias("pct_title_is_not_null_minus_is_explicit_flag"),
    )
)
