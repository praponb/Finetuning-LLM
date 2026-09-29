from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

derived = (
    filtered_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

derived_2 = (
    derived
    .filter(F.col("listener_id") <= 4)
    .select(
        "*",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
        F.when(F.col("ms_played") >= 301149, "HIGH").when(F.col("ms_played") >= 112786, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
)

result = (
    derived_2
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "is_skipped_label",
        "played_at_date",
        "ms_played_tier",
    )
    .orderBy(F.col("ms_played_tier").desc())
)
