from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

tagged = (
    scoped_tracks
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

screened = (
    tagged.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.array_contains(F.col("t.moods"), "chill")), "left_anti")
    .filter(~F.col("is_explicit"))
)

scored = (
    screened
    .filter(F.col("duration_sec") >= 164)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "days_since_release_date",
        "duration_sec_filled",
        "moods_count",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled_2"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.array_contains(F.col("moods"), "sad").alias("has_sad"),
    )
)

result = (
    scored
    .cube("is_explicit", "moods_count")
    .agg(F.grouping_id().alias("grouping_level"), F.count("*").alias("cnt"))
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("moods_count").asc_nulls_last(),
    )
)
