from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter((F.col("ms_played") >= 52009) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

labeled = (
    scoped
    .filter(F.col("source") == "radio")
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "source",
        "source_ref",
        "ms_played_filled",
        (F.col("ms_played") - F.col("ms_played_filled")).alias("ms_played_minus_ms_played_filled"),
        F.round(F.col("ms_played_filled") / F.nullif(F.col("ms_played"), F.lit(0)), 3).alias("ms_played_filled_per_ms_played"),
    )
)

qualified = (
    labeled.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.to_date(F.col("p.played_at")) < F.lit("2025-02-12").cast("date")), "left_semi")
)

tagged = (
    qualified
    .select(
        "id",
        "is_skipped",
        "source_ref",
        "ms_played_filled",
        "ms_played_minus_ms_played_filled",
        F.coalesce(F.col("ms_played_filled"), F.lit(0)).alias("ms_played_filled_filled"),
        F.substring(F.trim(F.col("source_ref")), 1, 2).alias("source_ref_prefix"),
    )
)

result = (
    tagged
    .select(
        "id",
        "is_skipped",
        "source_ref",
        "ms_played_filled",
        "ms_played_minus_ms_played_filled",
        "ms_played_filled_filled",
        "source_ref_prefix",
    )
    .orderBy("ms_played_minus_ms_played_filled", "ms_played_filled")
)
