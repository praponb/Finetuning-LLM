from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter((~F.col("is_explicit")) | (F.col("duration_sec") < 402))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

per_label = (
    valid
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        F.explode("moods").alias("mood"),
    )
)

derived = (
    per_label
    .filter(F.col("mood") != "focus")
    .select(
        "id",
        "release_date",
        "bpm",
        "mood",
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

result = (
    derived
    .select(
        "id",
        "mood",
        "release_date_dayofweek",
        "bpm_filled",
    )
    .unpivot(
        ids=["id", "mood"],
        values=["release_date_dayofweek", "bpm_filled"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
