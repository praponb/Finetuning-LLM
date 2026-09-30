from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .filter((F.col("source") == "album") | (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
        F.when(F.col("ms_played") >= 287769, "HIGH").when(F.col("ms_played") >= 108979, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
)

tagged = (
    scoped_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "played_at_label",
        "ms_played_tier",
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
        F.abs(F.col("ms_played") - 112786).alias("ms_played_distance"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

result = (
    tagged
    .select("id", "ms_played_distance", "ms_played")
    .unpivot(
        ids=["id"],
        values=["ms_played_distance", "ms_played"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
