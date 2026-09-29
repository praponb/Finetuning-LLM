from pyspark.sql import functions as F

valid = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
    )
)

flattened = (
    valid
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "release_date_week",
        F.posexplode("moods").alias("mood_pos", "mood"),
    )
)

derived = (
    flattened
    .filter(F.col("release_date").isNotNull())
    .select(
        "id",
        "bpm",
        "release_date_week",
        "mood_pos",
        "mood",
        (F.col("bpm") + F.col("mood_pos")).alias("bpm_plus_mood_pos"),
    )
)

enriched = derived.withColumn("release_date_week_quarter", F.quarter(F.col("release_date_week")))

scored = (
    enriched
    .select(
        "*",
        F.abs(F.col("bpm") - 118).alias("bpm_distance"),
        F.least(F.col("bpm_plus_mood_pos"), F.col("bpm")).alias("least_bpm_plus_mood_pos_bpm"),
        F.year(F.col("release_date_week")).alias("release_date_week_year"),
    )
)

result = (
    scored
    .select(
        "id",
        "mood_pos",
        "mood",
        "release_date_week_quarter",
        "least_bpm_plus_mood_pos_bpm",
        "release_date_week_year",
        F.date_format(F.col("release_date_week"), "yyyy-MM-dd HH:00").alias("release_date_week_label"),
        F.lower(F.regexp_replace(F.trim(F.col("mood")), " ", "_")).alias("mood_slug"),
    )
    .orderBy(F.col("mood").asc_nulls_last())
)
