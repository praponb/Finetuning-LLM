from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-05-12").cast("date"), F.lit("2025-04-02").cast("date"))) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        F.abs(F.col("bpm") - 100).alias("bpm_distance"),
    )
)

flattened = (
    prepared_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "release_date_month",
        "bpm_distance",
        F.explode("moods").alias("mood"),
    )
)

result = (
    flattened
    .select(
        "id",
        "mood",
        F.col("bpm_distance").alias("bpm_distance_v"),
        F.col("bpm").alias("m_bpm"),
    )
    .unpivot(
        ids=["id", "mood"],
        values=["bpm_distance_v", "m_bpm"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .orderBy("id", "mood", "measure")
)
