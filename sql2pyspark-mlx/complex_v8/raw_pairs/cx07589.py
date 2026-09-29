from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.col("bpm").between(85, 147)) | (F.col("duration_sec") >= 164))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
    )
)

enriched = (
    prepared_tracks
    .select(
        "id",
        "duration_sec",
        "release_date",
        "moods",
        "greatest_duration_sec_bpm",
        "title_clean",
        F.concat(F.col("title_clean"), F.lit("#"), F.col("id").cast("string")).alias("title_clean_ref"),
    )
)

result = (
    enriched
    .filter(F.col("duration_sec") > 165)
    .select(
        "id",
        "duration_sec",
        "release_date",
        "moods",
        "greatest_duration_sec_bpm",
    )
    .orderBy(F.col("duration_sec").asc_nulls_last())
)
