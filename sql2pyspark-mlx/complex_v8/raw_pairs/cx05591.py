from pyspark.sql import functions as F

cleaned = (
    df_tracks
    .filter((F.col("duration_sec").between(164, 365)) | (~F.col("is_explicit")))
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

annotated = (
    cleaned
    .select(
        "*",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
    )
)

screened = (
    annotated.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")) & (F.col("a.debut_year").between(1997, 2018)), "left_anti")
)

tagged = (
    screened
    .filter(F.col("duration_sec").between(128, 344))
    .select(
        "*",
        F.lower(F.trim(F.col("title"))).alias("title_clean_2"),
        F.least(F.col("duration_sec_filled"), F.col("bpm")).alias("least_duration_sec_filled_bpm"),
    )
)

result = (
    tagged
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "duration_sec_filled",
        "title_clean_2",
        "least_duration_sec_filled_bpm",
        F.size(F.col("moods")).alias("moods_count"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)
